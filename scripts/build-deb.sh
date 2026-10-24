#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
version="${1:-0.2.0}"
arch="${2:-$(dpkg --print-architecture)}"
./scripts/build-collector.sh "$version" "$arch"
root="$(mktemp -d "$PWD/build/deb.XXXXXX")"
transition="$(mktemp -d "$PWD/build/transition.XXXXXX")"
output="$(mktemp -d "$PWD/build/packages.XXXXXX")"
trap 'rm -rf "$root" "$transition" "$output"' EXIT HUP INT TERM
install -d "$root/DEBIAN" "$root/usr/bin" "$root/usr/sbin" "$root/etc/otelcol-extra/conf.d" \
    "$root/usr/lib/systemd/system" "$root/usr/lib/otelcol-extra" "$root/usr/share/doc/otelcol-extra" "$root/usr/share/otelcol-extra/examples"
install -m 0755 build/extra/otelcol-extra "$root/usr/bin/otelcol-extra"
install -m 0755 packaging/debian/otelcol-extra-set-credentials "$root/usr/sbin/otelcol-extra-set-credentials"
install -m 0755 packaging/debian/run "$root/usr/lib/otelcol-extra/run"
install -m 0644 packaging/debian/otelcol-extra.service "$root/usr/lib/systemd/system/otelcol-extra.service"
install -m 0644 distributions/extra/config.yaml "$root/etc/otelcol-extra/config.yaml"
install -m 0644 distributions/extra/config.yaml "$root/usr/share/otelcol-extra/config.default.yaml"
install -m 0644 distributions/extra/discord.yaml distributions/extra/switchbot.yaml distributions/extra/slack.yaml "$root/usr/share/otelcol-extra/examples/"
install -m 0644 LICENSE "$root/usr/share/doc/otelcol-extra/copyright"
for script in preinst postinst prerm postrm; do install -m 0755 "packaging/debian/$script" "$root/DEBIAN/$script"; done
printf '%s\n' /etc/otelcol-extra/config.yaml > "$root/DEBIAN/conffiles"
size="$(du -sk "$root" | cut -f1)"
cat > "$root/DEBIAN/control" <<EOF
Package: otelcol-extra
Version: $version
Architecture: $arch
Section: net
Priority: optional
Maintainer: Yoshi Yamaguchi <ymotongpoo@gmail.com>
Installed-Size: $size
Depends: ca-certificates, systemd (>= 250), python3
Suggests: alloy
Breaks: discord-otelcol (<< 0.2.0)
Replaces: discord-otelcol (<< 0.2.0)
Homepage: https://github.com/ymotongpoo/opentelemetry-collector-extra
Description: Extra OpenTelemetry Collector components
 Includes Discord, Slack and SwitchBot receivers, chat and S3 Tables exporters,
 OTLP and Prometheus receivers, and OTLP HTTP forwarding to Grafana Alloy.
 Includes a systemd service and encrypted credential registration.
EOF
mkdir -p "$transition/DEBIAN" dist
install -m 0755 packaging/debian/preinst "$transition/DEBIAN/preinst"
cat > "$transition/DEBIAN/control" <<EOF
Package: discord-otelcol
Version: $version
Architecture: all
Section: oldlibs
Priority: optional
Maintainer: Yoshi Yamaguchi <ymotongpoo@gmail.com>
Depends: otelcol-extra (>= $version)
Description: Transitional package for otelcol-extra
 Updates existing Discord Collector installations to the combined distribution.
 This package can be removed after the migration.
EOF
dpkg-deb --root-owner-group --build "$root" "$output/otelcol-extra_${version}_${arch}.deb"
dpkg-deb --root-owner-group --build "$transition" "$output/discord-otelcol_${version}_all.deb"
# Replace complete artifacts atomically; readers never see an incomplete archive.
mv "$output/otelcol-extra_${version}_${arch}.deb" "dist/otelcol-extra_${version}_${arch}.deb"
mv "$output/discord-otelcol_${version}_all.deb" "dist/discord-otelcol_${version}_all.deb"
