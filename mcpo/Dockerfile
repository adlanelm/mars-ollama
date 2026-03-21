FROM python:3.11-slim
WORKDIR /app

ARG USERNAME=appuser
ARG USER_UID=1000
ARG USER_GID=$USER_UID


# Install the latest version globally within the container
RUN apt-get update \
    && pip install --upgrade pip \
    && pip install uv mcpo markitdown-mcp \
    && apt-get install nodejs npm -y \
    && npm install -g @upstash/context7-mcp@latest 

# Create the user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    #
    # [Optional] Add sudo support. Omit if you don't need to install software after connecting.
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# ********************************************************
# * Anything else you want to do like clean up goes here *
# ********************************************************

# [Optional] Set the default
USER $USERNAME
RUN npx playwright install chrome 

# Default command to run when the container starts
#CMD ["uvx", "mcpo", "--host", "0.0.0.0", "--port", "8000", "--", "context7-mcp"]
CMD ["uvx", "mcpo", "--host", "0.0.0.0", "--port", "10001", "--config", "/config.json"]
