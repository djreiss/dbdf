dbdf
====

dbdf: an R package for big-data data.frames - treat database tables as if they were data.frames

====
This package is different from sqldf and other existing packages, in that it lets you create and 
manipulate database tables (currently SQLite and MySQL) *as if you were using a standard data.frame* --
i.e., using standard data.frame interfaces and function calls. 

It was developed so that I could take my existing R scripts which worked on data.frames, and 
apply them just as easily to tables stored in a database, without significant code changes.

It has grown and increased in complexity, but it currently allows you to do many (not all) standard
data.frame operations on the table. There are subset, and preliminary merge functions. Additionally,
a preliminary big-data vector that is stored in a database is also in here.

First, to install (in R):

```R
install.packages('devtools', dep=T)
require(devtools)
install_github('dbdf', 'dreiss-isb', subdir='dbdf')
```

====

Examples:

```
  dbdf.driver( "SQLite" ) ## use SQLite (default)
  dbdf.verbose( TRUE ) ## have dbdf print out all database operations
  s <- dbdf( iris, "iris", overwrite=T ) ## store 'iris' table to a sqlite database, file called 'iris'

  ## now all of these functions are fetching data directly from the database and outputting it:
  head(s)         ## calls 'select * from iris where row_idx between 1 and 6'
  s$Sepal.Width   ## calls 'select [row_idx],[row_names],[Sepal_Width] from iris'
  s[1,]           ## calls 'select * from iris where row_idx=1'
  s[,c('Sepal.Length','Species')]    ## calls 'select [row_idx],[row_names],[Sepal_Length],[Species] from iris'
  s[1,1]          ## calls 'select [row_idx],[row_names],[Sepal_Length] from iris where row_idx=1'; returns 5.1

  ## There are also functions for updating the database. First we need to allow it:
  s <- allow.write(s)
  s[1,1] <- 3     ## calls prepared query: 'update iris SET Sepal_Length=:V1 where row_idx=:R1'
  s[1,1]          ## now returns 3

  ## subset() allows you to create complex queries that are performed directly on the database.
  ## This runs the query: select [Species],[Sepal_Length] from [iris] where [Species]  in  ('virginica','versicolor') OR [Sepal_Length] <= 7.38905609893065'
  subset(s,Species%in%c('virginica','versicolor')|Sepal.Length<=`exp(2)`,c(Species,Sepal.Length))
```
  
Note that after closing your R session, the 'iris' database file still exists, so you can reconnect in a new session:

```
  dbdf.driver( "SQLite" ) ## use SQLite (default)
  dbdf.verbose( TRUE ) ## have dbdf print out all database operations
  s <- dbdf( filename="iris", tname="iris", overwrite=FALSE )
  print(s[1,1]) ## outputs 3 -- the value we set above.
```
