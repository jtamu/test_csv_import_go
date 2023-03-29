FROM public.ecr.aws/lambda/go:1

# Copy function code
COPY . ${LAMBDA_TASK_ROOT}

WORKDIR ${LAMBDA_TASK_ROOT}
# golangをinstall
RUN yum install -y golang zip && \
    go get && \
    go build -o main && \
    zip main.zip main

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "main" ]
