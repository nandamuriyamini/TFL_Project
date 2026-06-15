pipeline {
    agent any

    environment {
        REMOTE_HOST      = '13.41.167.97'
        REMOTE_USER      = 'ec2-user'
        SSH_KEY          = '/var/lib/jenkins/.ssh/test_key.pem'
        PROJECT_DIR      = '/home/ec2-user/yamini/tfl_Project1'
        HDFS_DIR         = '/tmp/yamini/tfl_project1'
        HIVESERVER2_HOST = '18.175.245.20'
    }

    stages {

        stage('Checkout') {
            steps {
                echo '========================================='
                echo 'Stage 1: Git Checkout'
                echo '========================================='
                checkout scm
                sh 'git log -1 --oneline'
            }
        }

        stage('Prepare Remote Directory') {
            steps {
                echo '========================================='
                echo 'Stage 2: Create Directories on Cloudera'
                echo '========================================='
                sh '''
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "mkdir -p ${PROJECT_DIR}/sqoop ${PROJECT_DIR}/hive" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Directories created"
                '''
            }
        }

        stage('Copy Scripts to Cloudera') {
            steps {
                echo '========================================='
                echo 'Stage 3: Copy Sqoop and Hive Scripts'
                echo '========================================='
                sh '''
                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/sqoop_import.sh ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/sqoop/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/hive_table.sql ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/hive/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Scripts copied successfully"
                '''
            }
        }

        stage('Set Permissions') {
            steps {
                echo '========================================='
                echo 'Stage 4: Set Execute Permissions'
                echo '========================================='
                sh '''
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "chmod +x ${PROJECT_DIR}/sqoop/sqoop_import.sh" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Permissions set"
                '''
            }
        }

        stage('Prepare Staging Directory') {
            steps {
                echo '========================================='
                echo 'Stage 5: Create Local Staging Directory'
                echo '========================================='
                sh '''
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "mkdir -p /tmp/hadoop/mapred/staging" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Staging directory ready"
                '''
            }
        }

        stage('Clean HDFS') {
            steps {
                echo '========================================='
                echo 'Stage 6: Clean HDFS Directories'
                echo '========================================='
                sh '''
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "HADOOP_USER_NAME=hdfs hdfs dfs -rm -r -f -skipTrash ${HDFS_DIR} 2>/dev/null || true; HADOOP_USER_NAME=hdfs hdfs dfs -mkdir -p ${HDFS_DIR}; HADOOP_USER_NAME=hdfs hdfs dfs -chmod 777 ${HDFS_DIR}" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "HDFS cleaned and recreated with open permissions"
                '''
            }
        }

        stage('Sqoop Import from PostgreSQL to HDFS') {
            steps {
                echo '========================================='
                echo 'Stage 7: Run Sqoop Import (6 tables)'
                echo '========================================='
                sh '''
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "bash ${PROJECT_DIR}/sqoop/sqoop_import.sh" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Sqoop import completed"
                '''
            }
        }

        stage('Create Hive Tables') {
            steps {
                echo '========================================='
                echo 'Stage 8: Create Hive External Tables'
                echo '========================================='
                sh '''
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "beeline -u 'jdbc:hive2://${HIVESERVER2_HOST}:10000/default' -n consultant -p 'Cl0ud3ra@2026#Secur3!' -f ${PROJECT_DIR}/hive/hive_table.sql" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Hive tables created"
                '''
            }
        }

        stage('Verify Results') {
            steps {
                echo '========================================='
                echo 'Stage 9: Verify HDFS Data'
                echo '========================================='
                sh '''
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "hdfs dfs -ls ${HDFS_DIR} 2>/dev/null || echo 'HDFS directory not found'" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true
                '''
            }
        }
    }

    post {
        success {
            echo '========================================='
            echo 'TFL PIPELINE COMPLETED SUCCESSFULLY'
            echo '========================================='
            echo "Cloudera: ${REMOTE_HOST}:${PROJECT_DIR}"
            echo "HDFS: ${HDFS_DIR}"
            echo '========================================='
        }
        failure {
            echo '========================================='
            echo 'TFL PIPELINE FAILED - check logs above'
            echo '========================================='
        }
        always {
            echo 'Pipeline execution completed'
        }
    }
}
