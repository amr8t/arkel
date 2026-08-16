+++
title = "Arkel"
+++

# Arkel

Arkel is a community platform for object storage. Node operators contribute storage in exchange for credit (USDC) and users can pay to use object storage. Arkel aims to be community driven as opposed to cloud providers that are purely profit driven.

More details on technical architecture coming soon. Checkout the [SourceCode](https://github.com/amr8t/arkelstore): 

## Get started

### As an operator

Run a **storage node** — contribute disk, get quota:

```sh
arkel storage \
  --addr 0.0.0.0:9001 \
  --index-addrs http://index.example:8001 \
  --data-dir /var/lib/arkel/storage
```

Run an **index node** (part of the Raft quorum):

```sh
arkel index \
  --http-addr 0.0.0.0:8001 \
  --peer-addresses <pubkey@ip:port,...>
```

### As a user

```sh
# put
arkel client put photo.jpg --bucket media --key holidays/1.jpg

# get
arkel client get media holidays/1.jpg --output photo.jpg

# rm
arkel client rm media holidays/1.jpg
```

See the [CLI reference](/cli/) for the full command surface.

Want to earn storage quota in exchange for contributing disk?
See [Contribute](/contribute/).
