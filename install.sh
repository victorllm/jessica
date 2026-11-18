#!/bin/sh
# Jessica installer.
#
# Installs jessica from a prebuilt release or builds it from this checkout,
# installs it into ~/.local/bin (or --bin-dir), prepares ~/.jessica (or
# --home), and can register a Jessica ACP provider with a local Paseo daemon.
#
# Usage: ./install.sh [options]
# Run with --help for the full option list.

set -eu

BIN_DIR="${JESSICA_BIN_DIR:-}"
DATA_DIR="${JESSICA_HOME:-}"
FORCE_CONFIG=0
WRITE_CONFIG=1
FROM_RELEASE=0
RELEASE_BASE_URL="${JESSICA_RELEASE_BASE_URL:-https://github.com/victorllm/jessica/releases/latest/download}"
RELEASE_PLATFORM=""
DOWNLOAD_DIR=""
INSTALL_SOURCE=""
PASEO=0
PASEO_HOME="${PASEO_HOME:-}"
PASEO_ENV_FILE=""
PASEO_ENV_CREATED=0
PASEO_LAUNCHER_KIND=""
PASEO_BACKUP=""
TMP_BIN=""
TMP_COMPAT=""
TMP_PASEO_CONFIG=""
TMP_PASEO_LAUNCHER=""
source_bin=""
source_version=""

usage() {
    cat <<'EOF'
Usage: install.sh [options]

Installs jessica, then prepares ~/.jessica. By default jessica is built from
this checkout; --from-release installs a prebuilt release instead.

The installer also creates a `jessica-cli` compatibility symlink next to the
installed `jessica` binary.

Options:
  --from-release           Download and install the latest prebuilt release
                           instead of building from this checkout
  --bin-dir <dir>          Install jessica here (default: ~/.local/bin)
  --home <dir>             Store configuration and runtime data here
                           (default: ~/.jessica)
  --force-config           Overwrite harness.toml, keeping a timestamped backup
  --no-config              Do not create or modify harness.toml
  --paseo                  Register a Jessica ACP provider with local Paseo
  --paseo-home <dir>       Paseo home containing config.json (default: ~/.paseo)
  --paseo-env-file <path>  Credential file read by the generated launcher
                           (default: <home>/paseo.env)
  -h, --help               Show this help

Environment:
  JESSICA_BIN_DIR, JESSICA_HOME, PASEO_HOME
  JESSICA_RELEASE_BASE_URL Override the release download base URL
                           (default: https://github.com/victorllm/jessica/releases/latest/download)

Without --from-release, the build uses `cargo build --release --locked -p
jessica-cli`. With --from-release, the installer downloads
jessica-<platform>.tar.gz and its .sha256 file, verifies the checksum before
installing, and currently supports linux-x86_64. A checksum detects corrupted
or tampered downloads; it is not a signature. If the download fails, run
./install.sh (without --from-release) to build from source.

The generated harness.toml keeps runtime files under ~/.jessica:

  harness.toml       configuration (created by this installer)
  events.db          append-only event log (created on first run)
  acp-state/         ACP session index (created by this installer)
  memory.db          layered memory, if enabled in harness.toml
  skills.db          skill store, if enabled in harness.toml

With --paseo, install.sh creates a jessica-paseo launcher and merges a
`jessica` provider into the Paseo config. An existing custom launcher is kept;
when it does not pass --config itself, install.sh adds the flag to the Paseo
provider command so that ACP sessions can be resumed.
EOF
}

die() {
    printf 'install.sh: %s\n' "$*" >&2
    exit 1
}

note() {
    printf '%s\n' "$*"
}

expand_tilde() {
    case "$1" in
        "~") printf '%s\n' "$HOME" ;;
        "~/"*) printf '%s/%s\n' "$HOME" "${1#"~/"}" ;;
        *) printf '%s\n' "$1" ;;
    esac
}

absolute_path() {
    case "$1" in
        /*) printf '%s\n' "$1" ;;
        *) printf '%s/%s\n' "$PWD" "$1" ;;
    esac
}

fetch_url() {
    url=$1
    out=$2
    if command -v curl >/dev/null 2>&1; then
        case "$url" in
            https://*)
                curl -fsSL --proto '=https' --proto-redir '=https' \
                    --connect-timeout 15 --max-time 300 --retry 2 \
                    -o "$out" "$url"
                ;;
            *)
                curl -fsSL --connect-timeout 15 --max-time 300 --retry 2 \
                    -o "$out" "$url"
                ;;
        esac
    elif command -v wget >/dev/null 2>&1; then
        wget -q --tries=2 --timeout=30 -O "$out" "$url"
    else
        die "curl or wget is required for --from-release"
    fi
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        die "sha256sum or shasum is required to verify the release download"
    fi
}

detect_release_platform() {
    os_name=$(uname -s 2>/dev/null) || die "failed to detect the operating system with uname"
    arch_name=$(uname -m 2>/dev/null) || die "failed to detect the CPU architecture with uname"
    case "$os_name/$arch_name" in
        Linux/x86_64|Linux/amd64)
            printf '%s\n' "linux-x86_64"
            ;;
        *)
            die "no prebuilt release for $os_name/$arch_name; run ./install.sh (without --from-release) to build from source"
            ;;
    esac
}

verify_checksum() {
    archive=$1
    checksum_file=$2
    [ -f "$checksum_file" ] || die "checksum file not found: $checksum_file"

    expected=$(awk 'NR == 1 { print $1 }' "$checksum_file")
    [ -n "$expected" ] || die "checksum file is empty: $checksum_file"
    case "$expected" in
        *[!0-9a-fA-F]*) die "invalid SHA-256 checksum in $checksum_file" ;;
    esac
    [ "${#expected}" -eq 64 ] || die "invalid SHA-256 checksum in $checksum_file"

    expected=$(printf '%s' "$expected" | tr 'A-F' 'a-f')
    actual=$(sha256_of "$archive")
    actual=$(printf '%s' "$actual" | tr 'A-F' 'a-f')
    if [ "$actual" != "$expected" ]; then
        die "SHA-256 mismatch for $(basename "$archive"): expected $expected, got $actual; refusing to install"
    fi
    note "Verified SHA-256: $actual"
}

prepare_release() {
    RELEASE_PLATFORM=$(detect_release_platform)
    release_asset="jessica-$RELEASE_PLATFORM.tar.gz"
    DOWNLOAD_DIR=$(mktemp -d "${TMPDIR:-/tmp}/jessica-release.XXXXXX") \
        || die "failed to create a temporary directory for the release download"
    release_archive="$DOWNLOAD_DIR/$release_asset"
    release_checksum="$DOWNLOAD_DIR/$release_asset.sha256"
    release_url="$RELEASE_BASE_URL/$release_asset"

    note "Downloading $release_url"
    if ! fetch_url "$release_url" "$release_archive"; then
        die "failed to download $release_url; check your network, or run ./install.sh (without --from-release) to build from source"
    fi
    if ! fetch_url "$release_url.sha256" "$release_checksum"; then
        die "failed to download $release_url.sha256; refusing to install without a checksum; run ./install.sh (without --from-release) to build from source"
    fi

    verify_checksum "$release_archive" "$release_checksum"

    tar -xzf "$release_archive" -C "$DOWNLOAD_DIR" jessica \
        || die "failed to extract $release_asset"
    if [ -L "$DOWNLOAD_DIR/jessica" ]; then
        die "$release_asset contains a symbolic link instead of a binary"
    fi
    [ -f "$DOWNLOAD_DIR/jessica" ] || die "$release_asset does not contain a jessica binary"
    chmod 0755 "$DOWNLOAD_DIR/jessica" || die "failed to make the downloaded jessica executable"

    source_bin="$DOWNLOAD_DIR/jessica"
    INSTALL_SOURCE="prebuilt release ($RELEASE_PLATFORM, SHA-256 verified)"
}

toml_string() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

write_config() {
    home_toml=$(toml_string "$DATA_DIR")

    cat > "$1" <<EOF
# Jessica harness configuration, generated by install.sh.
# Re-running install.sh keeps this file unless --force-config is used.

[events]
# Append-only event log. Required for --resume and ACP session recovery.
path = "$home_toml/events.db"

[acp]
# ACP session index. Requires [events].path.
state_dir = "$home_toml/acp-state"

[memory]
# Layered memory is disabled by default; enabling it keeps its database here.
enabled = false
path = "$home_toml/memory.db"

[skills]
# Skill distillation is disabled by default; enabling it keeps its database here.
enabled = false
path = "$home_toml/skills.db"

# The built-in tool allowlist is bash, read_file, and write_file. To restrict
# it, uncomment and edit:
#
# [policy]
# allowed_tools = ["read_file", "write_file"]
EOF
}

write_paseo_env_template() {
    cat > "$1" <<'EOF'
# Provider credentials for Jessica when it is launched by Paseo.
# Fill in one provider and run `paseo reload`.
#
# JESSICA_PROVIDER=openai
# OPENAI_API_KEY=...
# OPENAI_BASE_URL=...
# OPENAI_MODEL=...
#
# JESSICA_PROVIDER=anthropic
# ANTHROPIC_API_KEY=...
# ANTHROPIC_BASE_URL=...
# ANTHROPIC_MODEL=...
EOF
}

render_paseo_launcher() {
    out=$1
    cat > "$out" <<'WRAPPER'
#!/bin/sh
# Generated by Jessica's install.sh. Re-run `install.sh --paseo` to update.
set -eu

self_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-v" ]; then
    exec "$self_dir/jessica" --version
fi

jessica_home=${JESSICA_HOME:-$HOME/.jessica}
env_file=${JESSICA_PASEO_ENV_FILE:-$jessica_home/paseo.env}

if [ -r "$env_file" ]; then
    cr=$(printf '\r')
    while IFS= read -r line || [ -n "$line" ]; do
        line=${line%"$cr"}
        case "$line" in
            ''|'#'*) continue ;;
            *=*) ;;
            *) continue ;;
        esac

        key=${line%%=*}
        value=${line#*=}
        key=${key#export }
        key=$(printf '%s' "$key" | tr -d '[:space:]')

        case "$value" in
            \"*\") value=${value#\"}; value=${value%\"} ;;
            \'*\') value=${value#\'}; value=${value%\'} ;;
        esac
        value=$(printf '%s' "$value" | sed 's/^[[:space:]]*//')

        case "$key" in
            JESSICA_PROVIDER|OPENAI_API_KEY|OPENAI_BASE_URL|OPENAI_MODEL|ANTHROPIC_API_KEY|ANTHROPIC_BASE_URL|ANTHROPIC_MODEL)
                export "$key=$value"
                ;;
        esac
    done < "$env_file"
fi

exec "$self_dir/jessica" acp --config "$jessica_home/harness.toml" "$@"
WRAPPER
}

# Print the merged Paseo config. `generated` is 1 when the launcher was created
# by this script and therefore understands JESSICA_HOME and
# JESSICA_PASEO_ENV_FILE.
merge_paseo_config() {
    config=$1
    launcher=$2
    generated=$3
    env_file=$4
    inject_config=$5

    if command -v jq >/dev/null 2>&1; then
        jq --arg command "$launcher" \
            --arg home "$DATA_DIR" \
            --arg envfile "$env_file" \
            --arg generated "$generated" \
            --arg inject "$inject_config" \
            --arg harness "$DATA_DIR/harness.toml" '
            .agents = (.agents // {})
            | .agents.providers = (.agents.providers // {})
            | .agents.providers.jessica = ((.agents.providers.jessica // {})
                | if type == "object" then . else error("agents.providers.jessica is not an object") end)
            | .agents.providers.jessica.extends = "acp"
            | .agents.providers.jessica.label = "Jessica"
            | .agents.providers.jessica.description = "Deterministic Rust agent runtime"
            | .agents.providers.jessica.command =
                if $inject == "1" then [$command, "--config", $harness] else [$command] end
            | .agents.providers.jessica.supportsMcpServers = true
            | .agents.providers.jessica.additionalModels = (.agents.providers.jessica.additionalModels // [])
            | if $generated == "1" then
                  .agents.providers.jessica.env = ((.agents.providers.jessica.env // {}) + {
                      "JESSICA_HOME": $home,
                      "JESSICA_PASEO_ENV_FILE": $envfile
                  })
              else .
              end
        ' "$config"
    else
        python3 - "$config" "$launcher" "$generated" "$DATA_DIR" "$env_file" "$inject_config" <<'PY'
import json
import os
import sys

path, launcher, generated, jessica_home, env_file, inject_config = sys.argv[1:7]
with open(path, encoding="utf-8") as handle:
    config = json.load(handle)

agents = config.setdefault("agents", {})
if not isinstance(agents, dict):
    raise SystemExit("agents is not an object")
providers = agents.setdefault("providers", {})
if not isinstance(providers, dict):
    raise SystemExit("agents.providers is not an object")
entry = providers.setdefault("jessica", {})
if not isinstance(entry, dict):
    raise SystemExit("agents.providers.jessica is not an object")

entry["extends"] = "acp"
entry["label"] = "Jessica"
entry["description"] = "Deterministic Rust agent runtime"
if inject_config == "1":
    entry["command"] = [launcher, "--config", os.path.join(jessica_home, "harness.toml")]
else:
    entry["command"] = [launcher]
entry["supportsMcpServers"] = True
entry.setdefault("additionalModels", [])

if generated == "1":
    environment = entry.get("env", {})
    if not isinstance(environment, dict):
        raise SystemExit("agents.providers.jessica.env is not an object")
    environment["JESSICA_HOME"] = jessica_home
    environment["JESSICA_PASEO_ENV_FILE"] = env_file
    entry["env"] = environment

json.dump(config, sys.stdout, indent=2, ensure_ascii=False)
sys.stdout.write("\n")
PY
    fi
}

cleanup() {
    if [ -n "$TMP_BIN" ] && [ -f "$TMP_BIN" ]; then
        rm -f "$TMP_BIN"
    fi
    if [ -n "$TMP_COMPAT" ] && { [ -L "$TMP_COMPAT" ] || [ -e "$TMP_COMPAT" ]; }; then
        rm -f "$TMP_COMPAT"
    fi
    if [ -n "$TMP_PASEO_CONFIG" ] && [ -f "$TMP_PASEO_CONFIG" ]; then
        rm -f "$TMP_PASEO_CONFIG"
    fi
    if [ -n "$TMP_PASEO_LAUNCHER" ] && [ -f "$TMP_PASEO_LAUNCHER" ]; then
        rm -f "$TMP_PASEO_LAUNCHER"
    fi
    if [ -n "$DOWNLOAD_DIR" ] && [ -d "$DOWNLOAD_DIR" ]; then
        rm -rf "$DOWNLOAD_DIR"
    fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

while [ $# -gt 0 ]; do
    case "$1" in
        --bin-dir)
            [ $# -ge 2 ] || die "--bin-dir requires a value"
            BIN_DIR=$2
            shift 2
            ;;
        --bin-dir=*)
            BIN_DIR=${1#*=}
            shift
            ;;
        --home)
            [ $# -ge 2 ] || die "--home requires a value"
            DATA_DIR=$2
            shift 2
            ;;
        --home=*)
            DATA_DIR=${1#*=}
            shift
            ;;
        --from-release)
            FROM_RELEASE=1
            shift
            ;;
        --force-config)
            FORCE_CONFIG=1
            shift
            ;;
        --no-config)
            WRITE_CONFIG=0
            shift
            ;;
        --paseo)
            PASEO=1
            shift
            ;;
        --paseo-home)
            [ $# -ge 2 ] || die "--paseo-home requires a value"
            PASEO_HOME=$2
            PASEO=1
            shift 2
            ;;
        --paseo-home=*)
            PASEO_HOME=${1#*=}
            PASEO=1
            shift
            ;;
        --paseo-env-file)
            [ $# -ge 2 ] || die "--paseo-env-file requires a value"
            PASEO_ENV_FILE=$2
            PASEO=1
            shift 2
            ;;
        --paseo-env-file=*)
            PASEO_ENV_FILE=${1#*=}
            PASEO=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1 (try --help)"
            ;;
    esac
done

[ -n "${HOME:-}" ] || die "HOME must be set"
[ -n "$BIN_DIR" ] || BIN_DIR="$HOME/.local/bin"
[ -n "$DATA_DIR" ] || DATA_DIR="$HOME/.jessica"
if [ "$PASEO" -eq 1 ]; then
    [ -n "$PASEO_HOME" ] || PASEO_HOME="$HOME/.paseo"
fi

BIN_DIR=$(expand_tilde "$BIN_DIR")
DATA_DIR=$(expand_tilde "$DATA_DIR")
BIN_DIR=$(absolute_path "$BIN_DIR")
DATA_DIR=$(absolute_path "$DATA_DIR")

if [ -n "$PASEO_HOME" ]; then
    PASEO_HOME=$(expand_tilde "$PASEO_HOME")
    PASEO_HOME=$(absolute_path "$PASEO_HOME")
fi
if [ -n "$PASEO_ENV_FILE" ]; then
    PASEO_ENV_FILE=$(expand_tilde "$PASEO_ENV_FILE")
    PASEO_ENV_FILE=$(absolute_path "$PASEO_ENV_FILE")
fi

[ -n "$BIN_DIR" ] || die "--bin-dir must not be empty"
[ -n "$DATA_DIR" ] || die "--home must not be empty"
case "$BIN_DIR$DATA_DIR$PASEO_HOME$PASEO_ENV_FILE" in
    *'
'*) die "install paths must not contain newlines" ;;
esac

if [ "$PASEO" -eq 1 ] && [ "$WRITE_CONFIG" -eq 0 ]; then
    die "--paseo cannot be combined with --no-config"
fi

if [ "$PASEO" -eq 1 ]; then
    [ -f "$PASEO_HOME/config.json" ] || die "Paseo config not found: $PASEO_HOME/config.json (start Paseo once or pass --paseo-home)"
    if ! command -v jq >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1; then
        die "--paseo requires jq or python3 to update the Paseo config"
    fi
fi

RELEASE_BASE_URL=${RELEASE_BASE_URL%/}
[ -n "$RELEASE_BASE_URL" ] || die "JESSICA_RELEASE_BASE_URL must not be empty"
case "$RELEASE_BASE_URL" in
    *'
'*) die "JESSICA_RELEASE_BASE_URL must not contain newlines" ;;
esac

if [ "$FROM_RELEASE" -eq 1 ]; then
    prepare_release
else
    script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P) || die "cannot resolve the installer directory"
    REPO_ROOT=$script_dir
    [ -f "$REPO_ROOT/Cargo.toml" ] || die "install.sh must be run from the Jessica checkout (Cargo.toml not found)"
    [ -f "$REPO_ROOT/jessica-cli/Cargo.toml" ] || die "install.sh must be run from the Jessica checkout (jessica-cli not found)"

    find_cargo() {
        if command -v cargo >/dev/null 2>&1; then
            return 0
        fi
        if [ -f "$HOME/.cargo/env" ]; then
            . "$HOME/.cargo/env"
        fi
        command -v cargo >/dev/null 2>&1 || die "cargo was not found; install Rust from https://rustup.rs or add cargo to PATH"
    }

    find_cargo

    note "Building jessica (release) from $REPO_ROOT"
    if ! ( cd "$REPO_ROOT" && cargo build --release --locked -p jessica-cli ); then
        die "cargo build failed"
    fi

    target_dir=$(
        cd "$REPO_ROOT" || exit 1
        cargo metadata --format-version 1 --no-deps --manifest-path "$REPO_ROOT/Cargo.toml" 2>/dev/null \
            | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p' \
            | head -n 1
    ) || true
    [ -n "$target_dir" ] || target_dir="$REPO_ROOT/target"
    source_bin="$target_dir/release/jessica"

    [ -f "$source_bin" ] || die "build did not produce $source_bin"
    [ -x "$source_bin" ] || chmod 0755 "$source_bin"
    INSTALL_SOURCE="local build"
fi

if ! source_version=$("$source_bin" --version 2>/dev/null); then
    die "the jessica binary from the $INSTALL_SOURCE cannot run on this system"
fi
case "$source_version" in
    "jessica "*) ;;
    *) die "unexpected --version output from the $INSTALL_SOURCE: $source_version" ;;
esac
note "Using $source_version from the $INSTALL_SOURCE"

# --- Prepare ~/.jessica and the configuration -----------------------------

mkdir -p "$DATA_DIR" || die "failed to create $DATA_DIR"
chmod 700 "$DATA_DIR" 2>/dev/null || true

config_path="$DATA_DIR/harness.toml"

if [ "$WRITE_CONFIG" -eq 1 ]; then
    mkdir -p "$DATA_DIR/acp-state" || die "failed to create $DATA_DIR/acp-state"
    chmod 700 "$DATA_DIR/acp-state" 2>/dev/null || true

    if [ -f "$config_path" ] && [ "$FORCE_CONFIG" -eq 0 ]; then
        note "Keeping existing configuration: $config_path"
    else
        if [ -f "$config_path" ]; then
            backup_path="$config_path.bak.$(date +%Y%m%d%H%M%S)"
            cp -p "$config_path" "$backup_path" || die "failed to back up $config_path"
            note "Backed up configuration to $backup_path"
        fi

        tmp_config="$DATA_DIR/.harness.toml.$$"
        if ! write_config "$tmp_config"; then
            rm -f "$tmp_config"
            die "failed to write $config_path"
        fi
        chmod 600 "$tmp_config" 2>/dev/null || true
        mv -f "$tmp_config" "$config_path" || {
            rm -f "$tmp_config"
            die "failed to install $config_path"
        }
        note "Created configuration: $config_path"
    fi
fi

# --- Install the binary atomically ----------------------------------------

mkdir -p "$BIN_DIR" || die "failed to create $BIN_DIR"
installed_bin="$BIN_DIR/jessica"
TMP_BIN="$BIN_DIR/.jessica.$$"

cp "$source_bin" "$TMP_BIN" || die "failed to copy jessica into $BIN_DIR"
chmod 0755 "$TMP_BIN" || die "failed to make $TMP_BIN executable"
mv -f "$TMP_BIN" "$installed_bin" || die "failed to install $installed_bin"
TMP_BIN=""

# Task 7.0b: keep the old command name as a compatibility entry for at least
# one release cycle. The relative target keeps the pair working if BIN_DIR is
# moved as a whole.
compat_bin="$BIN_DIR/jessica-cli"
TMP_COMPAT="$BIN_DIR/.jessica-cli.$$"
rm -f "$TMP_COMPAT"
ln -s jessica "$TMP_COMPAT" || die "failed to create $compat_bin"
mv -f "$TMP_COMPAT" "$compat_bin" || die "failed to install $compat_bin"
TMP_COMPAT=""

# --- Optional local Paseo integration -------------------------------------

if [ "$PASEO" -eq 1 ]; then
    paseo_config="$PASEO_HOME/config.json"
    paseo_launcher="$BIN_DIR/jessica-paseo"
    generated_launcher=0
    inject_config=0

    if [ -f "$paseo_launcher" ] && ! grep -Fq "Generated by Jessica's install.sh" "$paseo_launcher"; then
        note "Keeping existing Paseo launcher: $paseo_launcher"
        PASEO_LAUNCHER_KIND="existing"
        if grep -Fq -- "--config" "$paseo_launcher"; then
            note "Existing launcher already supplies --config; leaving the provider command unchanged"
        else
            inject_config=1
            note "Paseo will pass --config \"$config_path\" to the existing launcher"
        fi
        if [ -n "$PASEO_ENV_FILE" ]; then
            note "warning: --paseo-env-file is ignored while an existing launcher is kept"
        fi
    else
        if [ -z "$PASEO_ENV_FILE" ]; then
            PASEO_ENV_FILE="$DATA_DIR/paseo.env"
        fi

        TMP_PASEO_LAUNCHER="$BIN_DIR/.jessica-paseo.$$"
        if ! render_paseo_launcher "$TMP_PASEO_LAUNCHER"; then
            rm -f "$TMP_PASEO_LAUNCHER"
            TMP_PASEO_LAUNCHER=""
            die "failed to create $paseo_launcher"
        fi
        chmod 0755 "$TMP_PASEO_LAUNCHER" || {
            rm -f "$TMP_PASEO_LAUNCHER"
            TMP_PASEO_LAUNCHER=""
            die "failed to make $paseo_launcher executable"
        }
        mv -f "$TMP_PASEO_LAUNCHER" "$paseo_launcher" || die "failed to install $paseo_launcher"
        TMP_PASEO_LAUNCHER=""
        generated_launcher=1
        PASEO_LAUNCHER_KIND="generated"

        if [ "$PASEO_ENV_FILE" = "$DATA_DIR/paseo.env" ] && [ ! -e "$PASEO_ENV_FILE" ]; then
            if ! write_paseo_env_template "$PASEO_ENV_FILE"; then
                rm -f "$PASEO_ENV_FILE"
                die "failed to create $PASEO_ENV_FILE"
            fi
            chmod 600 "$PASEO_ENV_FILE" 2>/dev/null || true
            PASEO_ENV_CREATED=1
            note "Created Paseo credential template: $PASEO_ENV_FILE"
        fi
    fi

    backup_path="$paseo_config.bak.$(date +%Y%m%d%H%M%S)"
    cp -p "$paseo_config" "$backup_path" || die "failed to back up $paseo_config"
    PASEO_BACKUP=$backup_path

    TMP_PASEO_CONFIG="$PASEO_HOME/.config.json.$$"
    if ! merge_paseo_config "$paseo_config" "$paseo_launcher" "$generated_launcher" "$PASEO_ENV_FILE" "$inject_config" > "$TMP_PASEO_CONFIG"; then
        rm -f "$TMP_PASEO_CONFIG"
        TMP_PASEO_CONFIG=""
        die "failed to update Paseo config: $paseo_config"
    fi
    [ -s "$TMP_PASEO_CONFIG" ] || {
        rm -f "$TMP_PASEO_CONFIG"
        TMP_PASEO_CONFIG=""
        die "Paseo config merge produced an empty file"
    }
    chmod 600 "$TMP_PASEO_CONFIG" 2>/dev/null || true
    mv -f "$TMP_PASEO_CONFIG" "$paseo_config" || {
        rm -f "$TMP_PASEO_CONFIG"
        TMP_PASEO_CONFIG=""
        die "failed to install $paseo_config"
    }
    TMP_PASEO_CONFIG=""
fi

# --- Report ---------------------------------------------------------------

note ""
note "Installed $source_version"
note "  source: $INSTALL_SOURCE"
note "  binary: $installed_bin"
note "  compatibility: $compat_bin -> jessica"
if [ "$WRITE_CONFIG" -eq 1 ]; then
    note "  config: $config_path"
fi
note "  data:   $DATA_DIR"

case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *)
        note ""
        note "Add $BIN_DIR to PATH, for example:"
        note "  export PATH=\"$BIN_DIR:\$PATH\""
        ;;
esac

note ""
note "Set OPENAI_API_KEY (and optionally OPENAI_BASE_URL / OPENAI_MODEL), then:"
if [ "$WRITE_CONFIG" -eq 1 ]; then
    note "  jessica --config \"$config_path\" \"your prompt\""
    note "  jessica acp --config \"$config_path\""
else
    note "  jessica --config <your-config> \"your prompt\""
    note "  jessica acp --config <your-config>"
fi

if [ "$PASEO" -eq 1 ]; then
    note ""
    note "Paseo integration"
    note "  config:   $paseo_config"
    note "  launcher: $paseo_launcher ($PASEO_LAUNCHER_KIND)"
    if [ "$inject_config" -eq 1 ]; then
        note "  arguments: --config \"$config_path\""
    fi
    note "  backup:   $PASEO_BACKUP"
    if [ "$PASEO_ENV_CREATED" -eq 1 ]; then
        note "  credentials: $PASEO_ENV_FILE (template; fill in provider settings)"
    elif [ "$generated_launcher" -eq 1 ]; then
        note "  credentials: $PASEO_ENV_FILE (read when present)"
    fi

    paseo_cmd="paseo"
    if [ "$PASEO_HOME" != "$HOME/.paseo" ]; then
        paseo_cmd="paseo --home \"$PASEO_HOME\""
    fi
    note ""
    note "Apply the Paseo configuration with:"
    note "  $paseo_cmd reload"
    note "Verify with:"
    note "  $paseo_cmd provider diagnostic jessica"
fi
