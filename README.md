# go-drmaa #
========

DRMAA job submission library for Go (#golang). Tested with Univa Grid Engine but 
should work also other resource manager / cluste scheduler. The "gestatus" library
only works with Grid Engine.

## Example ##

In case of Univa Grid Engine (other cluster scheduler are not tested yet):
   
   source /path/to/grid/engine/installation/default/settings.sh
   ./build
   cd example
   go build
   ./example
   
The example program submits a sleep job into the system and prints out detailed
job information as soon as the job is started.

More examples you can find at http://www.gridengine.eu.
