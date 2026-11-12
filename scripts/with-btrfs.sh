#!/bin/bash
# run a command as the caller, with temporary files on a disposable Btrfs mount

set -euo pipefail

if [[ "$#" -eq 0 ]]; then
    echo "usage: $0 COMMAND [ARG ...]" >&2
    exit 2
fi
if [[ "$(id -u)" -eq 0 ]]; then
    echo "with-btrfs: run as a non-root user with passwordless sudo" >&2
    exit 1
fi
for tool in mkfs.btrfs sudo mountpoint; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "with-btrfs: required command '$tool' is unavailable" >&2
        exit 127
    fi
done

# keep Unix socket fixture paths short, independently of the caller's TMPDIR
fixture="$(mktemp -d /tmp/rcp-btrfs.XXXXXX)"
mount_dir="$fixture/tmp"
cleanup() {
    local status=$? mount_status=0
    trap - EXIT INT TERM
    mountpoint -q "$mount_dir" || mount_status=$?
    case "$mount_status" in
        0)
            if ! sudo -n umount "$mount_dir"; then
                echo "with-btrfs: cannot unmount; preserved fixture at $fixture" >&2
                [[ "$status" -ne 0 ]] || status=1
                exit "$status"
            fi
            ;;
        32) ;;
        *)
            echo "with-btrfs: cannot verify mount state; preserved fixture at $fixture" >&2
            [[ "$status" -ne 0 ]] || status=1
            exit "$status"
            ;;
    esac
    if ! rm -rf -- "$fixture"; then
        [[ "$status" -ne 0 ]] || status=1
    fi
    exit "$status"
}
stop_command() {
    local group
    trap '' INT TERM
    for group in $(jobs -p); do
        kill -s "$1" -- "-$group" 2>/dev/null || true
    done
    wait || true
    exit "$2"
}
trap cleanup EXIT
trap 'stop_command INT 130' INT
trap 'stop_command TERM 143' TERM

mkdir "$mount_dir"
truncate -s 8G "$fixture/image"
mkfs.btrfs -q "$fixture/image"
sudo -n mount -t btrfs -o loop "$fixture/image" "$mount_dir"
if [[ "$(stat -f -c %T "$mount_dir")" != btrfs ]]; then
    echo "with-btrfs: temporary filesystem is not Btrfs" >&2
    exit 1
fi
sudo -n chown "$(id -u):$(id -g)" "$mount_dir"
echo "Btrfs test filesystem: $mount_dir (uid=$(id -u))"
# job control gives the command its own process group; the interruptible wait lets
# signal handlers terminate that group and reap the command before unmounting
set -m
TMPDIR="$mount_dir" "$@" &
wait "$!"
