#!/bin/bash

# riot commandline tool must be installed to path. It ships with apache jena -> https://jena.apache.org/download/index.cgi
mkdir -p ./processed_files/
for file in ./resultfiles/*.nt; do
  nt_filename=${file##*/}
  filename="${nt_filename%.*}"
  ttl_filename="${filename}.ttl"
  nt_output_path="./processed_files/${nt_filename}"
  ttl_output_path="./processed_files/${ttl_filename}"
  echo "processing ${nt_filename}"
  cat prefixes.ttl >"${nt_output_path}"
  sort "$file" | uniq >>"${nt_output_path}"
  riot --syntax=TURTLE --output=TURTLE "${nt_output_path}" >"${ttl_output_path}"
  rm "${nt_output_path}"
done
