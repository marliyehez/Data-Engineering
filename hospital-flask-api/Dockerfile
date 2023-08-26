# Base image
FROM python:3.7

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install -r requirements.txt

# Copy the application code to the container
COPY ./project .

# Set the command to run the Flask application
CMD ["flask", "run", "--host", "0.0.0.0", "--no-reload"]

