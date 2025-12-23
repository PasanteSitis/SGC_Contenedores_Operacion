FROM alpine/git:latest

RUN apk add --no-cache python3 py3-pip curl ca-certificates && \
    ln -sf /usr/bin/python3 /usr/bin/python

# copiar solo scripts para el servicio (sync/ está montado ro, pero para build copy helps)
COPY get_nc_editor.py /usr/local/bin/get_nc_editor.py
RUN chmod +x /usr/local/bin/get_nc_editor.py

ENTRYPOINT ["sh","-c"]
CMD ["/sync/sync_daemon.sh"]