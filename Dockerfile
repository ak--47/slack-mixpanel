# Use official Node.js 20 runtime as base image (required for File API used by undici)
FROM node:20-slim

# Set working directory
WORKDIR /usr/src/app

# Copy package files first for better Docker layer caching
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application source code
COPY src/ ./src/

# Create tmp directory for development output files
RUN mkdir -p tmp

# Expose the port the app runs on
EXPOSE 8080

# Set environment to production by default
ENV NODE_ENV=production

# Start the application directly with node (not npm) to avoid npm SIGTERM error logs
CMD ["node", "src/index.js"]