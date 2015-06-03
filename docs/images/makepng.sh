#!/bin/sh

for input in *.svg; do
    target=${input%.*}.png
    inkscape -z -e $target $input
done
