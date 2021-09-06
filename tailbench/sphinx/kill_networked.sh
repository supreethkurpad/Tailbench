#!/bin/bash

kill -9 $(cat server.pid)
kill -9 $(cat client.pid)

rm server.pid client.pid
