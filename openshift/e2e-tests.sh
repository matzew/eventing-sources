
# shellcheck disable=SC1090
source "$(dirname "$0")/../vendor/knative.dev/test-infra/scripts/e2e-tests.sh"
source "$(dirname "$0")/e2e-common.sh"

set -Eeuox pipefail

export TEST_IMAGE_TEMPLATE="${IMAGE_FORMAT//\$\{component\}/knative-eventing-test-{{.Name}}}"

env

scale_up_workers || exit 1

failed=0

(( !failed )) && install_strimzi || failed=1

(( !failed )) && install_serverless || failed=1

(( !failed )) && install_knative_eventing || failed=1

(( !failed )) && install_knative_kafka || failed=1

(( !failed )) && install_tracing || failed=1

(( !failed )) && run_e2e_tests || failed=1

(( !failed )) && uinstall_knative_kafka || failed=1

(( !failed )) && install_knative_kafka_channel_tls || failed=1

(( !failed )) && run_e2e_tls_tests || failed=1

(( !failed )) && uninstall_knative_kafka_channel || failed=1

(( failed )) && dump_cluster_state

(( failed )) && exit 1

success
