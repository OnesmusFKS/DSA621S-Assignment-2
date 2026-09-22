# restaurant: Dockerfile (STUB, comments only)
# 1. Base image: Ballerina (match distribution in Ballerina.toml: 2201.13.5)
#    Consider a multi-stage build: build stage with `bal build`, slim runtime stage with the JAR
# 2. Copy package files, run the build
# 3. EXPOSE 9002
# 4. Set CMD/ENTRYPOINT to run the built JAR
# 5. Config comes from env vars / mounted Config.toml, not baked into the image
# 6. Add a HEALTHCHECK hitting /health
