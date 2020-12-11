pipeline {
    agent none
    options { skipDefaultCheckout() }
    stages {
        stage("Linter and Tests") {
            agent any
            stages {
                stage('Prepare Workspace') {
                    steps {
                        sh """
                            env
                        """
                    }
                }
            }
        }
    }
}
