#!/bin/bash
lein check
lein cljfmt check
lein eastwood
lein kibit
lein test

