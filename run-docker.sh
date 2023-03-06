sudo docker build -t earthengine .

sudo docker run -it --name=ee-joao --rm -v "$(pwd)":"$(pwd)" earthengine