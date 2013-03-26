# go-drmaa #
========

DRMAA job submission library for Go (#golang). Tested with Univa Grid Engine but 
should work also other resource manager / cluster scheduler. The "gestatus" library
only works with Grid Engine (some values only available on Univa Grid Engine).

## Example ##

In case of Univa Grid Engine (other cluster scheduler are not tested yet):

~~~
   cd ~/go/src
   git clone git://github.com/dgruber/drmaa.git
   source /path/to/grid/engine/installation/default/settings.sh
   cd drmaa
   ./build
   cd example
   go build
   ./example
~~~

The example program submits a sleep job into the system and prints out detailed
job information as soon as the job is started.

More examples you can find at http://www.gridengine.eu.
