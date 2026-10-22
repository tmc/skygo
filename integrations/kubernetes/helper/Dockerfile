# The skygo-data helper image. Build it from the context written by build.sh,
# which holds only this file and the static helper executable (mode 0555).
FROM scratch
ARG HELPER_SHA256
LABEL dev.tmc.skygo.helper-sha256=${HELPER_SHA256}
COPY skygo-data /skygo-data
USER 65532:65532
ENTRYPOINT ["/skygo-data"]
