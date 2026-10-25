#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
version="${1:-0.4.0}"
arch="${2:-$(dpkg --print-architecture)}"
./scripts/build-collector.sh "$version" "$arch"
root="$(mktemp -d "$PWD/build/deb.XXXXXX")"
output="$(mktemp -d "$PWD/build/packages.XXXXXX")"
trap 'rm -rf "$root" "$output"' EXIT HUP INT TERM
install -d "$root/DEBIAN" "$root/usr/bin" "$root/usr/sbin" "$root/etc/otelcol-extra/conf.d" \
    "$root/usr/lib/systemd/system" "$root/usr/lib/otelcol-extra" "$root/usr/share/doc/otelcol-extra" "$root/usr/share/otelcol-extra/examples"
install -m 0755 build/extra/otelcol-extra "$root/usr/bin/otelcol-extra"
install -m 0755 packaging/debian/otelcol-extra-set-credentials "$root/usr/sbin/otelcol-extra-set-credentials"
install -m 0755 packaging/debian/run "$root/usr/lib/otelcol-extra/run"
install -m 0644 packaging/debian/otelcol-extra.service "$root/usr/lib/systemd/system/otelcol-extra.service"
install -m 0644 distributions/extra/config.yaml "$root/etc/otelcol-extra/config.yaml"
install -m 0644 distributions/extra/config.yaml "$root/usr/share/otelcol-extra/config.default.yaml"
install -m 0644 distributions/extra/discord.yaml distributions/extra/switchbot.yaml distributions/extra/slack.yaml distributions/extra/smartheim.yaml distributions/extra/openmeteo.yaml distributions/extra/jma.yaml "$root/usr/share/otelcol-extra/examples/"
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
 Includes Discord, Slack, SwitchBot, Smart Heim Navi, Open-Meteo and JMA receivers,
 chat and S3 Tables exporters,
 OTLP and Prometheus receivers, and OTLP HTTP forwarding to Grafana Alloy.
 Includes a systemd service and encrypted credential registration.
EOF
mkdir -p dist
dpkg-deb --root-owner-group --build "$root" "$output/otelcol-extra_${version}_${arch}.deb"
# Replace complete artifacts atomically; readers never see an incomplete archive.
mv "$output/otelcol-extra_${version}_${arch}.deb" "dist/otelcol-extra_${version}_${arch}.deb"
