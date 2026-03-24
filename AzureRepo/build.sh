#!/bin/sh
export PATH=/usr/bin:/bin:/usr/sbin:/sbin

check_exit_code() {
    if [ "$1" != "0" ]; then
        echo "$2: $1" 1>&2
        exit 1
    fi
}

TOOL="AzureRepo"
VERSION="2.0.2"

# Signing identities (set via environment or override here)
DEV_SIGNING_IDENTITY="Developer ID Application: UMB AG (DRMJHJA8YJ)"
PKG_SIGNING_IDENTITY="Developer ID Installer: UMB AG (DRMJHJA8YJ)"

# find the Xcode project
THISDIR=$(dirname "$0")
PROJ="${THISDIR}/${TOOL}.xcodeproj"
if [ ! -e "${PROJ}" ] ; then
    check_exit_code 1 "${PROJ} doesn't exist"
fi

# generate a revision number for from the list of Git revisions
GITREV=$(git log -n1 --format="%H" -- "${THISDIR}")
GITREVINDEX=$(git rev-list --count "$GITREV")
VERSION="${VERSION}.${GITREVINDEX}"

# make sure we have a clean build directory to use
BUILD_DIR="${THISDIR}/build"
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

# build the dylib
echo "Building ${TOOL}.plugin..."
xcodebuild build \
    -project "${PROJ}" \
    -configuration Release \
    -scheme "${TOOL}" \
    -destination "generic/platform=macOS" \
    -derivedDataPath "${BUILD_DIR}" \
    1>/dev/null

check_exit_code "$?" "Error building ${TOOL}.plugin"

# build a pkg (component pkg for now)

# make the payload (package root) dir
PKG_ROOT="${THISDIR}/payload"
mkdir -p "${PKG_ROOT}/usr/local/munki/repoplugins"
chmod -R 755 "${PKG_ROOT}"

# copy the dylib into the payload
cp "${BUILD_DIR}/Build/Products/Release/${TOOL}.plugin" "${PKG_ROOT}/usr/local/munki/repoplugins/"

# sign the plugin
echo "Signing ${TOOL}.plugin..."
codesign --force \
    --sign "${DEV_SIGNING_IDENTITY}" \
    "${PKG_ROOT}/usr/local/munki/repoplugins/${TOOL}.plugin"

check_exit_code "$?" "Error signing ${TOOL}.plugin"

# build the pkg!
echo "Building pkg for ${TOOL}..."
pkgbuild \
    --root "${PKG_ROOT}" \
    --identifier "com.googlecode.munki.${TOOL}" \
    --version "${VERSION}" \
    --ownership recommended \
    --sign "${PKG_SIGNING_IDENTITY}" \
    "${THISDIR}/${TOOL}-${VERSION}.pkg"

check_exit_code "$?" "Error building ${TOOL} pkg"

#if [ $? -eq 0 ] ; then
#    # clean up!
#    rm -r "$BUILD_DIR"
#    rm -r "$PKG_ROOT"
#fi