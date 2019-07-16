def flake8_container_image = "alpine/flake8:3.7.7"
def flake8_container_name = "pymapd-flake8-$BUILD_NUMBER"
def db_container_image = "omnisci/core-os-cuda-dev:master"
def db_container_name = "pymapd-db-$BUILD_NUMBER"
def testscript_container_image = "rapidsai/rapidsai:cuda10.0-runtime-ubuntu18.04"
def testscript_container_name = "pymapd-pytest-$BUILD_NUMBER"
def stage_succeeded

void setBuildStatus(String message, String state, String context) {
  step([
      $class: "GitHubCommitStatusSetter",
      reposSource: [$class: "ManuallyEnteredRepositorySource", url: "https://github.com/omnisci/pymapd"],
      contextSource: [$class: "ManuallyEnteredCommitContextSource", context: context],
      commitShaSource: [$class: "ManuallyEnteredShaSource", sha: "$GITHUB_PR_HEAD_SHA" ],
      errorHandlers: [[$class: "ChangingBuildStatusErrorHandler", result: "UNSTABLE"]],
      statusResultSource: [ $class: "ConditionalStatusResultSource", results: [[$class: "AnyBuildResult", message: message, state: state]] ]
  ]);
}

pipeline {
    agent none
    options { skipDefaultCheckout() }
    stages {
        stage('Set pending status') {
            agent any
            steps {
                // Set pending status manually for all jobs before node is started
                setBuildStatus("Build queued", "PENDING", "Flake8");
                setBuildStatus("Build queued", "PENDING", "Pytest - conda python3.6");
                setBuildStatus("Build queued", "PENDING", "Pytest - conda python3.7");
                setBuildStatus("Build queued", "PENDING", "Pytest - pip python3.6");
            }
        }
        stage("Linter and Tests") {
            agent { label 'centos7-p4-x86_64 && tools-docker' }
            stages {
                stage('Checkout') {
                    steps {
                        checkout scm
                    }
                }
                stage('Flake8') {
                    steps {
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            script { stage_succeeded = false }
                            setBuildStatus("Running tests", "PENDING", "$STAGE_NAME");
                            sh """
                                docker pull $flake8_container_image
                                docker run \
                                  --rm \
                                  --entrypoint= \
                                  --name $flake8_container_name \
                                  -v $WORKSPACE:/apps \
                                  $flake8_container_image \
                                    flake8
                                docker rm -f $flake8_container_name || true
                            """
                            script { stage_succeeded = true }
                        }
                    }
                    post {
                        always { 
                            script {
                                if (stage_succeeded == true) {
                                    setBuildStatus("Build succeeded", "SUCCESS", "$STAGE_NAME");
                                } else {
                                    sh """
                                        docker rm -f $flake8_container_name || true
                                    """
                                    setBuildStatus("Build failed", "FAILURE", "$STAGE_NAME");
                                }
                            }
                        }
                    }
                }
                stage('Prepare Workspace') {
                    steps {
                        sh """
                            # Pull required test docker container images
                            docker pull $db_container_image
                            docker pull $testscript_container_image

                            # Create docker network
                            docker network create pytest || true

                            # Update test server endpoint with db container name
                            #    NOTE: Single quotes (') have been replaced with <backslash><backslash>x27 to work in Jenkinsfile
                            sed -i "s/TSocket(\\"localhost\\", 6274)/TSocket(\\"${db_container_name}\\", 6274)/" tests/conftest.py
                            sed -i "s/host=\\x27localhost\\x27/host=\\x27${db_container_name}\\x27/" tests/conftest.py
                            sed -i "s|@localhost:6274|@${db_container_name}:6274|" tests/test_integration.py
                            sed -i "s|con._host == \\x27localhost\\x27|con._host == \\x27${db_container_name}\\x27|" tests/test_integration.py
                            sed -i "s|host=\\x27localhost\\x27|host=\\x27${db_container_name}\\x27|" tests/test_integration.py
                            sed -i "s/host=\\x27localhost\\x27/host=\\x27${db_container_name}\\x27/" tests/test_connection.py
                            sed -i "s|@localhost:6274|@${db_container_name}:6274|" tests/test_connection.py
                            sed -i "s/\"localhost\"/\"${db_container_name}\"/" tests/test_connection.py
                            sed -i "s/host=\\x27localhost\\x27/host=\\x27${db_container_name}\\x27/" pymapd/connection.py
                            sed -i "s|@localhost:6274|@${db_container_name}:6274|" pymapd/connection.py
                        """
                    }
                }
                stage('Pytest - conda python3.6') {
                    steps {
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            script { stage_succeeded = false }
                            setBuildStatus("Running tests", "PENDING", "$STAGE_NAME");
                            sh """
                                docker run \
                                  -d \
                                  --rm \
                                  --runtime=nvidia \
                                  --ipc="shareable" \
                                  --network="pytest" \
                                  -p 6273 \
                                  --name $db_container_name \
                                  $db_container_image
                                sleep 3

                                docker run \
                                  --rm \
                                  --runtime=nvidia \
                                  --ipc="container:${db_container_name}" \
                                  --network="pytest" \
                                  -v $WORKSPACE:/pymapd \
                                  --workdir="/pymapd" \
                                  --name $testscript_container_name \
                                  $testscript_container_image \
                                  bash -c '\
                                    PYTHON=3.6 ./ci/install-test-deps-conda.sh && \
                                    source activate /conda/envs/omnisci-dev && \
                                    pytest tests'

                                docker rm -f $testscript_container_name || true
                                docker rm -f $db_container_name || true
                            """
                            script { stage_succeeded = true }
                        }
                    }
                    post {
                        always { 
                            script {
                                if (stage_succeeded == true) {
                                    setBuildStatus("Build succeeded", "SUCCESS", "$STAGE_NAME");
                                } else {
                                    sh """
                                        docker rm -f $testscript_container_name || true
                                        docker rm -f $db_container_name || true
                                    """
                                    setBuildStatus("Build failed", "FAILURE", "$STAGE_NAME");
                                }
                            }
                        }
                    }
                }
                stage('Pytest - conda python3.7') {
                    steps {
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            script { stage_succeeded = false }
                            setBuildStatus("Running tests", "PENDING", "$STAGE_NAME");
                            sh """
                                docker run \
                                  -d \
                                  --rm \
                                  --runtime=nvidia \
                                  --ipc="shareable" \
                                  --network="pytest" \
                                  -p 6273 \
                                  --name $db_container_name \
                                  $db_container_image
                                sleep 3

                                docker run \
                                  --rm \
                                  --runtime=nvidia \
                                  --ipc="container:${db_container_name}" \
                                  --network="pytest" \
                                  -v $WORKSPACE:/pymapd \
                                  --workdir="/pymapd" \
                                  --name $testscript_container_name \
                                  $testscript_container_image \
                                  bash -c '\
                                    PYTHON=3.7 ./ci/install-test-deps-conda.sh && \
                                    source activate /conda/envs/omnisci-dev && \
                                    pytest tests'

                                docker rm -f $testscript_container_name || true
                                docker rm -f $db_container_name || true
                            """
                            script { stage_succeeded = true }
                        }
                    }
                    post {
                        always { 
                            script {
                                if (stage_succeeded == true) {
                                    setBuildStatus("Build succeeded", "SUCCESS", "$STAGE_NAME");
                                } else {
                                    sh """
                                        docker rm -f $testscript_container_name || true
                                        docker rm -f $db_container_name || true
                                    """
                                    setBuildStatus("Build failed", "FAILURE", "$STAGE_NAME");
                                }
                            }
                        }
                    }
                }
                stage('Pytest - pip python3.6') {
                    steps {
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            script { stage_succeeded = false }
                            setBuildStatus("Running tests", "PENDING", "$STAGE_NAME");
                            sh """
                                docker run \
                                  -d \
                                  --rm \
                                  --runtime=nvidia \
                                  --ipc="shareable" \
                                  --network="pytest" \
                                  -p 6273 \
                                  --name $db_container_name \
                                  $db_container_image
                                sleep 3

                                docker run \
                                  --rm \
                                  --runtime=nvidia \
                                  --ipc="container:${db_container_name}" \
                                  --network="pytest" \
                                  -v $WORKSPACE:/pymapd \
                                  --workdir="/pymapd" \
                                  --name $testscript_container_name \
                                  $testscript_container_image \
                                  bash -c '\
                                    . ~/.bashrc && \
                                    conda install python=3.6 -y && \
                                    ./ci/install-test-deps-pip.sh && \
                                    pytest tests'

                                docker rm -f $testscript_container_name || true
                                docker rm -f $db_container_name || true
                            """
                            script { stage_succeeded = true }
                        }
                    }
                    post {
                        always { 
                            script {
                                if (stage_succeeded == true) {
                                    setBuildStatus("Build succeeded", "SUCCESS", "$STAGE_NAME");
                                } else {
                                    sh """
                                        docker rm -f $testscript_container_name || true
                                        docker rm -f $db_container_name || true
                                    """
                                    setBuildStatus("Build failed", "FAILURE", "$STAGE_NAME");
                                }
                            }
                        }
                    }
                }
                // stage('Pytest - pip python3.7') {
                //     steps {
                //         catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                //             script { stage_succeeded = false }
                //             setBuildStatus("Running tests", "PENDING", "$STAGE_NAME");
                //             sh """
                //                 docker run \
                //                   -d \
                //                   --rm \
                //                   --runtime=nvidia \
                //                   --ipc="shareable" \
                //                   --network="pytest" \
                //                   -p 6273 \
                //                   --name $db_container_name \
                //                   $db_container_image
                //                 sleep 3

                //                 docker run \
                //                   --rm \
                //                   --runtime=nvidia \
                //                   --ipc="container:${db_container_name}" \
                //                   --network="pytest" \
                //                   -v $WORKSPACE:/pymapd \
                //                   --workdir="/pymapd" \
                //                   --name $testscript_container_name \
                //                   $testscript_container_image \
                //                   bash -c '\
                //                     . ~/.bashrc && \
                //                     conda install python=3.7 -y && \
                //                     ./ci/install-test-deps-pip.sh && \
                //                     pytest tests'

                //                 docker rm -f $testscript_container_name || true
                //                 docker rm -f $db_container_name || true
                //             """
                //             script { stage_succeeded = true }
                //         }
                //     }
                //     post {
                //         always { 
                //             script {
                //                 if (stage_succeeded == true) {
                //                     setBuildStatus("Build succeeded", "SUCCESS", "$STAGE_NAME");
                //                 } else {
                //                     sh """
                //                         docker rm -f $testscript_container_name || true
                //                         docker rm -f $db_container_name || true
                //                     """
                //                     setBuildStatus("Build failed", "FAILURE", "$STAGE_NAME");
                //                 }
                //             }
                //         }
                //     }
                // }
            }
            post {
                always {
                    sh """
                        docker rm -f $flake8_container_name || true
                        docker rm -f $testscript_container_name || true
                        docker rm -f $db_container_name || true
                        sudo chown -R jenkins-slave:jenkins-slave $WORKSPACE
                    """
                    cleanWs()
                }
            }
        }
    }
}
