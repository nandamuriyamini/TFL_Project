pipeline {
    agent any

    environment {
        REMOTE_HOST      = '13.41.167.97'
        REMOTE_USER      = 'consultant'
        REMOTE_PASSWORD  = 'Cl0ud3ra@2026#Secur3!'
        PROJECT_DIR      = '/home/consultant/yamini/tfl_Project1'
        HDFS_DIR         = '/tmp/yamini/tfl_project1'
        HDFS_FULL_LOAD   = '/tmp/yamini/tfl_full_load'
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
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "mkdir -p ${PROJECT_DIR}/sqoop ${PROJECT_DIR}/hive ${PROJECT_DIR}/spark" 2>&1 | \
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
                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/sqoop_import.sh ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/sqoop/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/hive_table.sql ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/hive/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/spark/spark_full_load_tfl.py ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/spark/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/spark/hive_full_load_table.sql ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/spark/ 2>&1 | \
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
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
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
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
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
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "HADOOP_USER_NAME=hdfs hdfs dfs -rm -r -f -skipTrash ${HDFS_DIR} 2>/dev/null || true; \
                         HADOOP_USER_NAME=hdfs hdfs dfs -mkdir -p ${HDFS_DIR}; \
                         HADOOP_USER_NAME=hdfs hdfs dfs -chmod 777 ${HDFS_DIR}; \
                         HADOOP_USER_NAME=hdfs hdfs dfs -rm -r -f -skipTrash ${HDFS_FULL_LOAD} 2>/dev/null || true; \
                         HADOOP_USER_NAME=hdfs hdfs dfs -mkdir -p ${HDFS_FULL_LOAD}/output; \
                         HADOOP_USER_NAME=hdfs hdfs dfs -chmod -R 777 ${HDFS_FULL_LOAD}" 2>&1 | \
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
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
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
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "beeline -u 'jdbc:hive2://${HIVESERVER2_HOST}:10000/default' -n consultant -p 'Cl0ud3ra@2026#Secur3!' -f ${PROJECT_DIR}/hive/hive_table.sql" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Hive tables created"
                '''
            }
        }

        stage('Run Spark Full Load') {
            steps {
                echo '========================================='
                echo 'Stage 9: Spark Full Load - Kafka → HDFS Parquet (batch)'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "spark-submit \
                             --master local[4] \
                             --name TFL_Full_Load \
                             --driver-memory 2g \
                             --conf spark.sql.parquet.writeLegacyFormat=true \
                             --conf spark.sql.shuffle.partitions=4 \
                             ${PROJECT_DIR}/spark/spark_full_load_tfl.py" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Full load completed"
                '''
            }
        }

        stage('Create Hive Full Load Table') {
            steps {
                echo '========================================='
                echo 'Stage 10: Register Hive Full Load Table'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "echo '--- Full Load HDFS Output ---'; \
                         hdfs dfs -ls ${HDFS_FULL_LOAD}/output 2>/dev/null || echo 'No output found'; \
                         echo '--- Full Load Size ---'; \
                         hdfs dfs -du -s -h ${HDFS_FULL_LOAD}/output 2>/dev/null || echo '0 bytes'; \
                         echo '--- Creating Hive Full Load Table ---'; \
                         beeline -u 'jdbc:hive2://${HIVESERVER2_HOST}:10000/default' -n consultant -p 'Cl0ud3ra@2026#Secur3!' -f ${PROJECT_DIR}/spark/hive_full_load_table.sql" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true
                '''
            }
        }

        stage('Verify Results') {
            steps {
                echo '========================================='
                echo 'Stage 11: Verify HDFS Data'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
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
            echo "HDFS Sqoop : ${HDFS_DIR}"
            echo "HDFS Full Load: ${HDFS_FULL_LOAD}/output"
            echo "Hive Full Load: yamini_tfl_proj.tfl_full_load"
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
