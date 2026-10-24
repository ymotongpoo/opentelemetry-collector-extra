#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
version="${1:-0.1.1}"
arch="${2:-$(dpkg --print-architecture)}"
./scripts/build-discord-collector.sh "$version" "$arch"

root="$(mktemp -d "$PWD/build/deb.XXXXXX")"
trap 'rm -rf "$root"' EXIT HUP INT TERM
install -d "$root/DEBIAN" "$root/usr/bin" "$root/usr/sbin" "$root/etc/discord-otelcol" \
    "$root/usr/lib/systemd/system" "$root/usr/share/doc/discord-otelcol"
install -m 0755 build/discord/discord-otelcol "$root/usr/bin/discord-otelcol"
install -m 0644 distributions/discord/config.yaml "$root/etc/discord-otelcol/config.yaml"
install -m 0755 packaging/debian/discord-otelcol-set-token "$root/usr/sbin/discord-otelcol-set-token"
install -m 0644 packaging/debian/discord-otelcol.service "$root/usr/lib/systemd/system/discord-otelcol.service"
install -m 0644 LICENSE "$root/usr/share/doc/discord-otelcol/copyright"
for script in postinst prerm postrm; do
    install -m 0755 "packaging/debian/$script" "$root/DEBIAN/$script"
done
printf '%s\n' /etc/discord-otelcol/config.yaml > "$root/DEBIAN/conffiles"
size="$(du -sk "$root" | cut -f1)"
cat > "$root/DEBIAN/control" <<EOF
Package: discord-otelcol
Version: $version
Architecture: $arch
Section: net
Priority: optional
Maintainer: Yoshi Yamaguchi <ymotongpoo@gmail.com>
Installed-Size: $size
Depends: ca-certificates, systemd (>= 250), python3
Suggests: alloy
Homepage: https://github.com/ymotongpoo/opentelemetry-collector-extra
Description: Discord metrics collector for Grafana Alloy
 Collects Discord message metrics and forwards them over OTLP/HTTP to
 a local Grafana Alloy instance. Includes a systemd service and an
 interactive tool to store the bot token as an encrypted credential.
EOF
mkdir -p dist
dpkg-deb --root-owner-group --build "$root" "dist/discord-otelcol_${version}_${arch}.deb"
