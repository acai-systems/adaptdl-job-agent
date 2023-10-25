# Use the official Python 3 base image
ARG model
FROM python:3

# Set the working directory
WORKDIR /app

# Copy the script and requirements file to the container
COPY create_adaptdl_job.py /app/
COPY adaptdl_agent_requirements.txt /app/
COPY launcher_script.sh /app/

# Expose the port the REST API will run on
EXPOSE 37654

RUN chmod 0777 launcher_script.sh

CMD ["./launcher_script.sh"]
