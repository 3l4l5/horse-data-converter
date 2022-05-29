cd ..
PROJECT_DIR=/project/
if [ -e $PROJECT_DIR ]; then
    cp -rf  horse-data-scraper/ $PROJECT_DIR
else
    mkdir /project/
    cp -rf  horse-data-scraper/ $PROJECT_DIR
fi
