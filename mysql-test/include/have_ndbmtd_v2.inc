#
# Skip test if ndbmtd_v2 binary is not available
#

if (!$HAVE_NDBMTD_V2) {
  --skip Test requires ndbmtd_v2 binary
}
