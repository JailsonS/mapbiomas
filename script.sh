#!/bin/bash

echo 'Iniciando o docker com a credencial:' $1

cd /
cd /root/.config/earthengine

echo 'Lista de credenciais válidas:'
ls -l

echo 'Configurando a credencial'
cp $1/credentials .

earthengine ls