FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    make \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements and Makefile
COPY requirements.txt Makefile ./

# Use Makefile to install Python dependencies
RUN make install

# Copy the Streamlit app
COPY telco-streamlite.py .

# Expose Streamlit default port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "telco-streamlite.py"]
