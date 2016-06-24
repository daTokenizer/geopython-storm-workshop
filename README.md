# geopython-storm-workshop


to set up please follow instructions on:

* http://leiningen.org/ → scroll down to Install 
* http://redis.io/ → click Download → scroll down to Install
* https://github.com/andymccurdy/redis-py → clone repo locally → cd into the project → sudo python setup.py install
* http://streamparse.readthedocs.io/ → click Quickstart


then start your redis server using `redis-server`
and in the root directory of this repo type `sparse run`
 
# OK now that it's running, what should I do next?
1. try and extend the existing topology (adding the dehydrator is a good warmup, the bolt is already written)
2. replace some of the logic with your own logic
3. create a topology of your own - try it, it won't bite
