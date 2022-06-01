cd ..
PROJECT_DIR=/project/
if [ -e $PROJECT_DIR ]; then
    cp -rf  horse-data-converter/ $PROJECT_DIR
else
    mkdir /project/
    cp -rf  horse-data-converter/ $PROJECT_DIR
fi
cd horse-data-converter
bash docker_build.sh