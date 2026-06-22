# Use the official oven/bun:alpine base image
FROM oven/bun:alpine

WORKDIR /app

# Create tmp directory and set permissions for the bun user
RUN mkdir -p /work/tmp && chown -R bun:bun /work/tmp

# Copy package.json first to leverage Docker layer caching
COPY package.json ./
RUN bun install --production

# Copy the rest of the application code
COPY . .

# Ensure the bun user owns the app directory
RUN chown -R bun:bun /app

USER bun

EXPOSE 3000

# Start the application using Bun
CMD ["bun", "index.js"]
