# Setup guide

## How to access the SSH server

1. Add the following config to `~/.ssh/config`:

```ssh
Host pilotscope
	User pilotscope
	Hostname 0.tcp.ap.ngrok.io
	Port 11448
```

2. Log into the server with the command:

```bash
ssh pilotscope
```

## How to test

0. Before testing, you can develop locally or on the SSH server, and then push your changes to the remote.

1. SSH to the server, then log into the container with the command:

```bash
ssh container
```

The password is `pilotscope`.

2. Start the conda environment with:

```bash
conda activate pilotscope
```

3. The code is in `~/PilotScopeCore`. Pull the changes from the remote, then run the commands you'd like to test.
