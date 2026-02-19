FROM ghcr.io/elbe0116/qubership-docker-integration-tests:PSUPATPOS-63

# Switch to root for installation and for EKS/S3 compatibility (same as Kafka)
USER root

ENV ROBOT_OUTPUT=/opt/robot/output \
    SERVICE_CHECKER_SCRIPT=${ROBOT_HOME}/rabbitmq_pod_checker.py \
    SERVICE_CHECKER_SCRIPT_TIMEOUT=500

ENV STATUS_CUSTOM_RESOURCE_GROUP=apps
ENV STATUS_CUSTOM_RESOURCE_VERSION=v1
ENV STATUS_CUSTOM_RESOURCE_PLURAL=deployments
ENV STATUS_CUSTOM_RESOURCE_NAME=rabbitmq-integration-tests

RUN mkdir -p ${ROBOT_OUTPUT}

COPY requirements.txt ${ROBOT_HOME}/requirements.txt
COPY rabbitmq_pod_checker.py ${ROBOT_HOME}/rabbitmq_pod_checker.py
COPY write_status_wrapper.py ${ROBOT_HOME}/write_status_wrapper.py
COPY robot ${ROBOT_HOME}

RUN set -x \
    && pip3 install -r ${ROBOT_HOME}/requirements.txt \
    && rm -rf /var/cache/apk/*

# Fix permissions for non-root user (1000:0) for AWS EKS and S3 adapter scripts
RUN chown -R 1000:0 ${ROBOT_HOME} && chmod -R 775 ${ROBOT_HOME}

USER 1000:0

WORKDIR ${ROBOT_HOME}
EXPOSE 8080
VOLUME ["${ROBOT_OUTPUT}"]
