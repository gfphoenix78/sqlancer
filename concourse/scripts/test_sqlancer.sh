#!/bin/bash -l
set -exo pipefail

OLDPATH=${PATH}
echo "OLDPATH = ${OLDPATH}"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts

source "${GPDB_CONCOURSE_DIR}/common.bash"

function install_deps() {
yum install -y maven
}
function prepare_sqlancer() {
pushd ${CWDIR}/../../
#mvn package -DskipTests
popd
}
function test_run() {
  cat > /home/gpadmin/test_run.sh <<-EOF
#!/bin/bash -l
set -exo pipefail
source /usr/local/greenplum-db-devel/greenplum_path.sh
source ${TOP_DIR}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
gpconfig -c optimizer -v $OPTIMIZER
gpstop -air

pushd ${CWDIR}/../../
createdb test
mvn package -DskipTests
cd target
java -jar sqlancer-*.jar --num-threads 4 --username gpadmin postgres --connection-url postgresql://localhost:\$PGPORT/mytestdb
popd
EOF

  pushd /home/gpadmin
    chown gpadmin:gpadmin  test_run.sh ${CWDIR}/../..
    chmod a+x  test_run.sh
  popd
  su gpadmin -c "/bin/bash /home/gpadmin/test_run.sh"
}

function setup_gpadmin_user() {
    ${GPDB_CONCOURSE_DIR}/setup_gpadmin_user.bash
}

function _main() {
    time install_deps
    time prepare_sqlancer

    time install_gpdb
    time setup_gpadmin_user
    time make_cluster

    time test_run
}

_main "$@"

