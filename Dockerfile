FROM node:20-slim

# python3/make/g++ needed for better-sqlite3 native build
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./

# Install ALL deps (including devDeps) so tsc is available for the build step
RUN npm install

COPY tsconfig.json ./
COPY src ./src

# Compile TypeScript → dist/
RUN npm run build

# /data is where the SQLite volume will be mounted
RUN mkdir -p /data

EXPOSE 3000

CMD ["npm", "start"]
