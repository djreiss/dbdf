##################################################################################
## dbdf - SQLite-enabled data frame wrapper (also possible to use MySQL)
## Copyright (C) David J Reiss, Institute for Systems Biology
##                      dreiss@systemsbiology.org
## This software is provided AS IS with no warranty expressed or implied. Neither 
## the authors of this software nor the Institute for Systems Biology shall be held
## liable for anything that happens as a result of using this software
###################################################################################

## NOTE: get list of all functions applied to data.frame via methods(class="data.frame")
##
## TODO DONE: add indexes for speedy lookup (default, rnames&indexes only; allow user set "all" or specific columns)
## TODO DONE: add a 1:nrow(df) column for speedy lookup of rows by integer index
## TODO DONE: store an open connection in the dbdf object? Test if it needs to be opened again for future access.
## TODO DONE: dbWriteTable replaces '.' in column names with '_' (among other things) in the sqlite db table.
##     Need to map these names back to the original column names
## TODO DONE: a select() function to run a basic select SQL query on the table and return the result
## TODO DONE: implement a recursive as.dbdf.list() function to convert everything in a list to an dbdf if it is
##         bigger than a certain size (say 2048 bytes via object.size())
## TODO DONE: if use ":memory:" as filename, need to keep connection open always!
## TODO DONE: apply.dbdf can be run (when MAR==1) in data.frame-converted "chunks" of e.g. 1000 at a time
## TODO DONE: cbind.dbdf() should set the additional column names correctly!
## TODO DONE (implemented in dbdf() with df=input.file.name): a generic file.to.dbdf() file importer that uses
##         read.func=read.delim() (as an option) and optionally reads and appends to the table in chunks
## TODO DONE: override '[<-.dbdf' to allow updating values in the table.
## TODO DONE: allow duplicating an dbdf (which duplicates the table to a nes table). DONE if "df" arg to
##         dbdf() is another dbdf (using rbind.dbdf)!
## TODO DONE: let rbind.dbdf() rbind an dbdf to another dbdf! Also, don't have rbind.dbdf() update the table of
##         s - instead have it create a new dbdf with a new table
## TODO DONE: use dbGetPreparedQuery instead of loop for '[<-.dbdf', e.g.:
##      dbGetPreparedQuery(con,"update iris set Species=:spec where row_idx=:idx",data.frame(spec=150:1,idx=1:150))
## TODO DONE(?): if query ends up being too many characters, break up selection (e.g. in [.dbdf) into smaller pieces
##         and rbind them together.
## TODO DONE(?): make [<-.dbdf accept 2-col matrix as "row" param. (no col param) and set corresponding values
## TODO DONE: Add a 'read.only' flag to each dbdf to prevent changing its database accidentally
## TODO DONE: select() should be able to take more than 1 dbdf (and allow joins and other such things)...
##      Not necessary - see "merge" below.
## TODO: rbind.dbdf and cbind.dbdf should use "..." for multiple args and expand things that are too short and
##     allow first arg to not be a dbdf (e.g. rbind(-1,s,iris) doesn't work right now)
## TODO: have rm.dbdf() (with null "s" param) remove all tables for which no dbdf obj. in memory has any link
##     Along these lines - maybe a global table ("workspace") that stores all references to all db files/tables?
## TODO (?): Add dbBeginTransaction() and dbCommit() surrounding multiple queries in one function -
##     Only applicable to SQLite?
## TODO: implement other data.frame funcs, esp. Ops, Math(?), duplicated(?), transform(?), aggregate(?),
##      merge(done, mostly!), subset(done, mostly!), plot, stack, unstack, and
##      attaching ( methods(class="data.frame") shows all data.frame methods )
## BIG TODO: Ideally each command that returns a data.frame should have the option of returning
##      a dbdf object (associated with a new temporary(?) table). See unused code in merge() for how to
#       potentially do this for SQLite.
## BIG TODO (NEARLYDONE?): dbv vector that either (1) creates its own 1-column table, or
##       (2) uses only a single column (as an attribute) in an existing table.
## TODO (alternative to 2 points just above): Allow "virtual" dbdf's which contain a query string that
#       is only executed upon an 'as.data.frame()' call to it.
## BIG TODO (mostly done for MySQL!!): Allow for use of RMySQL and PostgreSQL (see http://rdbi.sourceforge.net/)
##       as alternatives to RSQLite

if ( ! "dbdf" %in% search() ) {
  require( RSQLite )
  try( require( RMySQL ) )
  dbdf <- attach( NULL, name="dbdf" )
  sys.source( "~/scratch/halo/generic_scripts/dbdf.R", env=dbdf )
  stop()
}

open.con <- function( filename=":memory:", drv=dbdf.driver() ) {
  if ( drv == "SQLite" ) require( RSQLite )
  else if ( drv == "MySQL" ) require( RMySQL )
  dbname <- filename
  if ( is.dbdf( filename ) ) {
    s <- filename
    if ( ! is.null( attr( s, "driver" ) ) ) drv <- attr( s, "driver" )
    filename <- attr( s, "filename" )
    if ( ! is.null( attr( s, "dbname" ) ) ) dbname <- attr( s, "dbname" )
    else dbname <- filename
  }

  m <- DBI:::dbDriver( drv )
  cons <- dbListConnections( m )

  if ( drv == "SQLite" ) {
    if ( filename == "mem" || is.null( filename ) ) filename <- dbname <- ":memory:" ## Shortcut for in-memory SQLite db
    f.exists <- filename == ":memory:" || file.exists( filename ) || drv != "SQLite"
    if ( ! f.exists && ! file.exists( dirname( filename ) ) ) dir.create( dirname( filename ), recursive=T )
  } else if ( drv == "MySQL" ) { ## "Group" name is not always same as dbname; but only dbname gets stored in
    tmp.con <- dbConnect( m, group=filename ) ## connection info, so we need to get the real dbname here.
    dbname <- dbGetInfo( tmp.con )$dbname
    dbDisconnect( tmp.con )
  }
  tabs <- lapply( cons, function( i ) dbGetInfo( i )$dbname )
  exists.already <- dbname %in% unlist( tabs )
  if ( exists.already ) {
    wh <- which( sapply( tabs, function( i ) dbname %in% i ) )
    con <- cons[[ wh[ 1 ] ]]
    attr( con, "not.new" ) <- TRUE
  } else {
    if ( dbdf.verbose() ) cat( "DBDF: Opening", drv, "connection to:", filename, "\n" )
    if ( drv == "SQLite" ) con <- dbConnect( m, dbname=filename )
    else if ( drv == "MySQL" ) con <- dbConnect( m, group=filename ) ## Use ~/.my.cnf as in RMySQL docs
    attr( con, "not.new" ) <- filename == ":memory:"
  }
  con
}

close.con <- function( con, force=F ) {
  try( {
    open.rs <- dbGetInfo( con )$rsId
    if ( length( open.rs ) > 0 ) sapply( open.rs, dbClearResult )
  } )
  if ( force || ! attr( con, "not.new" ) ) {
    if ( dbdf.verbose() ) cat( "DBDF: Closing", attr( class( con ), "package" ), "connection to:",
                              dbGetInfo( con )$dbname, "\n" )
    dbDisconnect( con )
  }
}

drop.all.tables <- function( con ) { ## Just for my own use
  if ( is.dbdf( con ) ) { con <- open.con( con ); on.exit( close.con( con ) ) }
  for ( i in dbListTables( con ) ) { cat( "Dropping:", i, "\n" ); dbGetQuery( con, paste( "drop table", i ) ) }
}

my.query <- function( con, qq, ... ) {
  if ( dbdf.verbose() ) cat( "DBDF:", qq, "\n" )
  dbGetQuery( con, qq, ... )
}

reshape.dbdf.out <- function( out, s ) {
  if ( is.null( out ) || sum( dim( out ) ) == 0 ) return( out ) 
  rnames <- make.unique( out$row_names )
  if ( is.dbv( s ) ) { out <- out[ ,3 ]; names( out ) <- rnames; return( out ) }
  rownames( out ) <- make.unique( rnames )
  out <- out[ ,-(1:2) ,drop=F ]
  names( out ) <- names( s )
  out
}

get.colname.mapping <- function( s, con ) {
  tmp <- my.query( con, paste( "select * from ", attr( s, "tname" ), "_colname_map", sep="" ) )
  mapping <- tmp[ ,2 ]; names( mapping ) <- tmp[ ,1 ]
  mapping
}

if ( FALSE ) {
  source( "construct.package.R" )
  construct.package("dbdf","0.0.4",require=c("RSQLite","DBI"), suggest="RMySQL",
                functions.ex=c("test.dbdf","drop.all.tables"),
                functions.hid=c("my.query","close.con","open.con","reshape.dbdf.out","get.colname.mapping","driver",
                  "write.default.meta.info","setMetaInfo"))
}

write.default.meta.info <- function( s, type=c("create","access","update")[1], ... ) {
  meta <- c( date(), Sys.info()[ "user" ] )
  names( meta ) <- paste( type, c( "date", "user" ), sep="." )
  setMetaInfo( s, meta, ... )
}

test.dbdf <- function( ... ) {
  dbdf.driver( "SQLite" )
  ##dbdf( iris, "iris", overwrite=T, ... )
  s<<-dbdf(iris,":memory:",rw="rw");vv<<-dbv2(s,"Species");v<<-dbv(1:100,":memory:")
}

##### ALL PUBLIC FUNCTIONS ARE BELOW #####

dbdf.verbose <- function( v ) {
  if ( missing( v ) ) {
    out <- options( "dbdf.verbose" )$dbdf.verbose
    if ( is.null( out ) ) options( dbdf.verbose=F )
    return( options( "dbdf.verbose" )$dbdf.verbose )
  }
  options( dbdf.verbose=v )
}

options( dbdf.verbose=TRUE )

dbdf.driver <- function( d="SQLite" ) {
  if ( missing( d ) ) return( options( "dbdf.driver" )$dbdf.driver )
  options( dbdf.driver=d )
  if ( d == "SQLite" ) require( RSQLite )
  else if ( d == "MySQL" ) require( RMySQL )
  options( "dbdf.driver" )$dbdf.driver
}

if ( is.null( options( "dbdf.driver" )$dbdf.driver ) ) dbdf.driver( "SQLite" )
##on.exit( lapply( DBI::dbListConnections( dbDriver( dbdf.driver() ) ), close.con, force=T ) )

driver <- function( s, ... ) UseMethod( "driver" )

## Get selected driver for given dbdf obj... allows dbdf's for SQLite and MySQL to exist in workspace
##  at same time!
driver.dbdf <- function( s ) {
  if ( ! is.null( attr( s, "driver" ) ) ) return( attr( s, "driver" ) )
  dbdf.driver()
}

## "Default" database name for given driver... for SQLIte it's ".dbdf/%s.db" where %s is (by default) the
##   name of the input table. For MySQL it's "dbdf" which (from ~/.my.cnf) selected database "dreiss" on
##   localhost (with username and password undisclosed!)
dbdf.database <- function( dbname=c(SQLite=".dbdf/%s.db",MySQL="dbdf"), driver=dbdf.driver() ) {
  if ( ! missing( dbname ) ) {
    if ( driver %in% names( dbname ) ) options( dbdf.dbname=dbname[ driver ] )
    else options( dbdf.dbname=dbname )
  } else if ( is.null( options( "dbdf.dbname" )$dbdf.dbname ) ) { ##||
             ##options( "dbdf.dbname" )$dbdf.dbname != dbname[ driver ] ) {
    options( dbdf.dbname=dbname[ driver ] )
  }
  options( "dbdf.dbname" )$dbdf.dbname
}

if ( is.null( options( "dbdf.dbname" )$dbdf.dbname ) ) dbdf.database( driver=dbdf.driver() )

## Allow storage of meta info (e.g. date created, date accessed/updated, user updated/accessed, etc)
setMetaInfo <- function( s, ... ) UseMethod( "setMetaInfo" )
getMetaInfo <- function( s, ... ) UseMethod( "getMetaInfo" )

setMetaInfo.dbdf <- function( s, ... ) {
  con <- open.con( s ); on.exit( close.con( con ) )
  tname <- paste( attr( s, "tname" ), "meta_info", sep="_" )
  tmp <- data.frame( name=names( unlist( list( ... ) ) ),
                    meta_value=as.character( sapply( unlist( list( ... ) ), function( i ) i ) ) )
  tmp <- cbind( row_names=tmp[ ,1 ], tmp )
  if ( dbExistsTable( con, tname ) ) {
    if ( dbdf.verbose() ) cat( "Reading", dbdf.driver(), "table", tname, "\n" )
    meta <- dbReadTable( con, tname )
    meta <- cbind( row_names=rownames( meta ), meta )
    meta <- merge( tmp, meta, all=T )
  } else {
    meta <- tmp
  }
  if ( dbdf.verbose() ) cat( "DBDF: writing meta info to", dbdf.driver(), "table", tname, "\n" )
  dbWriteTable( con, tname, meta, row.names=F, append=F, overwrite=T )
  meta
}

getMetaInfo <- function( s, ... ) {
  tname <- paste( attr( s, "tname" ), "meta_info", sep="_" )
  con <- open.con( s ); on.exit( close.con( con ) )
  if ( ! dbExistsTable( con, tname ) ) return( list() )
  if ( dbdf.verbose() ) cat( "Reading meta info from", dbdf.driver(), "table", tname, "\n" )
  out <- dbReadTable( con, tname )
  if ( length( list( ... ) ) > 0 ) out <- out[ unlist( list( ... ) ), ]
  tmp <- as.list( as.character( out[ ,2 ] ) ); names( tmp ) <- as.character( out[ ,1 ] )
  tmp
}

## Enable global setting of whether to store meta info automatically (can slow down things)
dbdf.meta <- function( m ) {
  if ( missing( m ) ) {
    out <- options( "dbdf.meta" )$dbdf.meta
    if ( is.null( out ) ) options( dbdf.meta=F )
    return( options( "dbdf.meta" )$dbdf.meta )
  }
  options( dbdf.meta=m )
}

options( dbdf.meta=FALSE )

## Input "df" can be a data frame (normally); a filename pointing to a tsv/csv; a vector; another
##   dbdf (in which case the table is copied); or NULL in which case a new dbdf object pointing to
#    an already-created DBDF table (via filename/tname) is returned
dbdf <- function( df, filename=NULL, overwrite=F, index="default", tname=NULL, chunk.size=100, read.func=read.delim,
                 rw="r", driver=dbdf.driver(), ... ) {
  if ( missing( df ) || is.null( df ) ) {
    ## Create the correct dbdf "structure" for an "orphaned" db file/table that already exists
    ##  (given filename, tname)
    ## TODO: If it is just a table that exists in the database (without row_idx,row_names columns or a
    ##  colname_mapping table) then copy it, create those things and use it for a new dbdf.
    out <- list()
    attr( out, "class" ) <- c( "dbdf", "data.frame" )
    attr( out, "filename" ) <- filename
    attr( out, "tname" ) <- tname
    con <- open.con( filename, driver ); on.exit( close.con( con ) )
    tmp <- my.query( con, paste( "select * from", attr( out, "tname" ), "limit 1" ) )
    tmp <- tmp[ ,! colnames( tmp ) %in% c( "row_idx", "row_names" ) ]
    attr( out, "ncol" ) <- ncol( tmp )
    attr( out, "nrow" ) <- my.query( con, paste( "select count(*) from", tname ) )[ ,1 ]
    ##attr( out, "ncol" ) <- ncol( my.query( con, paste( "select * from", tname, "limit 1" ) ) )
    attr( out, "rw" ) <- rw
    attr( out, "driver" ) <- driver
    if ( dbdf.meta() ) write.default.meta.info( out, "create" )
    return( out )
  }

  ### Read in dbdf from a tsv/csv file in chunks
  ## See this for a way to read in file faster -- is there a way to do this from R?
  ## https://stackoverflow.com/questions/6324434/how-do-i-speed-up-the-import-of-data-from-a-csv-file-into-a-sqlite-table-in-win
  else if ( ( is.character( df ) && file.exists( df ) ) || "connection" %in% class( df ) ) {
    orig.con <- NULL
    if ( ! "connection" %in% class( df ) ) {
      if ( is.null( filename ) ) {
        ##if ( driver == "SQLite" ) filename <- paste( ".dbdf/", DBI:::make.db.names.default( df ), ".db", sep="" )
        ##else filename <- DBI:::make.db.names.default( df )
        filename <- sprintf( dbdf.database( driver=driver ), DBI:::make.db.names.default( df ) )
      }
      if ( is.null( tname ) ) tname <- DBI:::make.db.names.default( df )
    } else {
      filename <- sprintf( dbdf.database( driver=driver ), DBI:::make.db.names.default( summary( df )$description ) )
      tname <- DBI:::make.db.names.default( summary( df )$description )
      orig.con <- df
    }
    filename <- gsub( "[\"']", "", filename )
    tname <- gsub( "[\"']", "", tname )
    con <- open.con( filename, driver ); on.exit( close.con( con ) )
    new.df <- tmp <- NULL; skip <- 0
    while( is.null( tmp ) || nrow( tmp ) == chunk.size ) {
      if ( ! is.null( orig.con ) ) df <- orig.con
      tmp <- read.func( df, skip=skip, nrow=chunk.size, header=(skip==0), ... )
      if ( skip == 0 ) {
        cnames <- colnames( tmp )
        new.dbdf <- dbdf( tmp, filename, overwrite=T, index=index, tname=tname, ... )
      } else {
        colnames( tmp ) <- cnames
        if ( all( rownames( tmp ) == as.character( 1:nrow( tmp ) ) ) )
            rownames( tmp ) <- skip:( skip + nrow(tmp) - 1 ) + 1
        tmp <- cbind( row_idx=skip:( skip + nrow(tmp) - 1 ) + 1, row_names=rownames( tmp ), tmp )
        if ( dbdf.verbose() ) cat( "DBDF: Appending", nrow( tmp ), "rows to table", tname, "in file", filename, "\n" )
        dbWriteTable( con, tname, tmp, row.names=F, append=T )
      }
      skip <- skip + nrow( tmp )
    }
    attr( new.dbdf, "nrow" ) <- skip + 1
    if ( driver == "MySQL" ) attr( new.dbdf, "dbname" ) <- dbGetInfo( con )$dbname ## Could be different
    if ( dbdf.meta() ) write.default.meta.info( new.dbdf, "create" )
    return( new.dbdf )
  }

  else if ( is.dbdf( df ) ) {
    ## Copy the original dbdf to a new table/object in chunks
    if ( is.null( filename ) ) filename <- attr( df, "filename" )
    if ( is.null( tname ) ) tname <- attr( df, "tname" )
    chunk.size <- min( nrow( df ), chunk.size )
    out <- dbdf( as.data.frame( df[ 1:chunk.size, ] ), filename=filename, tname=tname, driver=driver, ... )
    if ( nrow( out ) == nrow( df ) ) return( out )

    filename <- attr( out, "filename" )
    tname <- attr( out, "tname" )
    con <- open.con( filename, driver ); on.exit( close.con( con ) )
    ch.inds <- c( seq( 1, nrow( df ), by=chunk.size ), nrow( df ) + 1 )

    for ( i in 2:( length( ch.inds ) - 1 ) ) {
      tmp <- as.data.frame( df[ ch.inds[ i ]:( ch.inds[ i+1 ] - 1 ), ] )
      tmp <- cbind( row_idx=ch.inds[ i ]:( ch.inds[ i+1 ] - 1 ), row_names=rownames( tmp ), tmp )
      if ( dbdf.verbose() ) cat( "DBDF: Appending", nrow( tmp ), "rows to table", tname, "in file", filename, "\n" )
      dbWriteTable( con, tname, tmp, row.names=F, append=T )
    }
    attr( out, "nrow" ) <- my.query( con, paste( "select count(row_idx) from", attr( out, "tname" ) ) )[ ,1 ]
    tmp <- my.query( con, paste( "select * from", attr( out, "tname" ), "limit 1" ) )
    tmp <- tmp[ ,! colnames( tmp ) %in% c( "row_idx", "row_names" ) ]
    attr( out, "ncol" ) <- ncol( tmp )
    attr( out, "driver" ) <- driver
    if ( driver == "MySQL" ) attr( out, "dbname" ) <- dbGetInfo( con )$dbname ## Could be different
    if ( dbdf.meta() ) write.default.meta.info( out, "create" )
    return( out )
  } 
  
  else if ( is.null( filename ) ) {
    ##if ( driver == "SQLite" )
    ##  filename <- paste( ".dbdf/", DBI:::make.db.names.default( deparse( substitute( df ) ) ), ".db", sep="" )
    ##else filename <- DBI:::make.db.names.default( deparse( substitute( df ) ) )
    filename <- sprintf( dbdf.database( driver=driver ), DBI:::make.db.names.default( deparse( substitute( df ) ) ) )
  }
  if ( is.null( tname ) ) tname <- DBI:::make.db.names.default( deparse( substitute( df ) ) )
  else tname <- DBI:::make.db.names.default( tname )

  if ( ! is.data.frame( df ) ) df <- as.data.frame( df )
  out <- list()
  attr( out, "class" ) <- c( "dbdf", "data.frame" )
  attr( out, "filename" ) <- filename
  attr( out, "tname" ) <- tname
  attr( out, "nrow" ) <- nrow( df )
  attr( out, "ncol" ) <- ncol( df )
  attr( out, "rw" ) <- rw
  attr( out, "driver" ) <- driver
  con <- open.con( filename, driver ); on.exit( close.con( con ) )
  if ( driver == "MySQL" ) {
    attr( out, "dbname" ) <- dbGetInfo( con )$dbname ## Could be (probably is) different
    colnames( df ) <- make.unique( substr( colnames( df ), 1, 64 ) ) ## MySQL has a 64-char limit on column names
  }

  t.exists <- dbExistsTable( con, tname )
  if ( t.exists && ! overwrite ) { ## If table exists, append an integer number to the end of its name
    orig.tname <- tname
    ind <- 1
    while( dbExistsTable( con, tname ) ) {
      tname <- paste( orig.tname, "_", ind, sep="" )
      ind <- ind + 1
    }
    attr( out, "tname" ) <- tname
    t.exists <- FALSE
  }
  if ( ! t.exists || overwrite ) {
    if ( dbdf.verbose() ) cat( "DBDF: Creating", driver, "table", tname, "in DB", filename, "\n" )
    if ( is.null( rownames( df ) ) || length( rownames( df ) ) == 0 )
      rownames( df ) <- make.unique( as.character( df[ ,1 ] ) )
    dbWriteTable( con, tname, cbind( row_idx=1:nrow( df ), row_names=rownames( df ), df ), row.names=F, overwrite=T )
    new.colnames <- my.query( con, paste( "select * from", tname, "limit 1" ) )
    new.colnames <- colnames( new.colnames )[ -(1:2) ]
    if ( dbdf.verbose() ) cat( "DBDF: Creating", driver, "table", paste( tname, "colname_map", sep="_" ),
                              "in DB", filename, "\n" )
    dbWriteTable( con, paste( tname, "colname_map", sep="_" ), data.frame( orig=colnames( df ), new=new.colnames ),
                 row.names=F, overwrite=T )
    idx.columns <- paste( c( "row_idx", "row_names" ), collapse="," ) ## default
    if ( driver == "MySQL" ) idx.columns <- paste( c( "row_idx", paste( "row_names(",
                      max(nchar(rownames(df))), ")", sep="" ) ), collapse="," ) ## default
    if ( index[ 1 ] == "all" ) {
      if ( driver == "SQLite" ) new.colnames <- paste( "[", new.colnames, "]", sep="" )
      idx.columns <- paste( c( idx.columns, paste( new.colnames, collapse="," ) ), collapse="," ) ## all
    } else if ( index[ 1 ] != "default" ) {
      new.idx <- new.colnames[ colnames( df ) %in% index ]
      if ( driver == "SQLite" ) new.idx <- paste( "[", new.idx, "]", sep="" )
      idx.columns <- paste( idx.columns, c( paste( new.idx, collapse="," ) ), sep="," ) ## given
    }
    my.query( con, paste( "create index RDB_", tname, "_INDEX on ", tname, "(", idx.columns, ")", sep="" ) )
    if ( dbdf.meta() ) write.default.meta.info( out, "create" )
  } else {
    if ( dbdf.meta() ) write.default.meta.info( out, "update" )
  }
  out
}

as.data.frame.dbdf <- function( s, row.names=NULL, optional=FALSE, ... ) s[]
is.dbdf <- function( s ) 'dbdf' %in% class( s )
is.dbv <- function( v ) "dbv" %in% class( v )
as.matrix.dbdf <- function( s ) as.matrix( as.data.frame( s ) )
as.vector.dbdf <- function( s, mode="any" ) as.vector( as.matrix( s ), mode=mode )
dim.dbdf <- function( s ) c( attr( s, "nrow" ), attr( s, "ncol" ) )
length.dbdf <- function( s ) ncol( s )
as.list.dbdf <- function( s ) as.list( as.data.frame( s ) )
allow.write <- function( s, rw=T ) { attr( s, "rw" ) <- if ( rw ) "rw" else "r"; s }
head.dbdf <- function( s, n=6, ... ) s[ 1:n, ]
tail.dbdf <- function( s, n=6, ... ) s[ ( nrow(s) - n + 1 ):nrow( s ), ]
str.dbdf <- function( s ) str( s[ 1:11, ] )

print.dbdf <- function( s, n=nrow(s), ... ) {
  cat( "DBDF stored in table ", attr( s, "tname" ), " in ", driver( s ), " DB ", attr( s, "filename" ), ":\n",
      sep="" )
  if ( ! is.dbv( s ) && n != nrow( s ) ) print( head( s, n=n, ... ) )
  else if ( is.dbv( s ) && n != length( s ) ) print( head( s, n=n, ... ) )
  else print( s[] )
}

names.dbdf <- function( s ) {
  con <- open.con( s ); on.exit( close.con( con ) )
  my.query( con, paste( "select orig from ", attr( s, "tname" ), "_colname_map", sep="" ) )[ ,1 ]
}

dimnames.dbdf <- function( s ) { ## called by rownames(s) and colnames(s) -- a waste!
  con <- open.con( s ); on.exit( close.con( con ) )
  rnames <- my.query( con, paste( "select row_names from", attr( s, "tname" ) ) )[ ,1 ]
  if ( is.dbv( s ) ) return( list( rnames, NULL ) )
  cnames <- names( s )
  list( rnames, cnames )
}

"$.dbdf" <- function( s, name ) s[[ name ]]
"[[.dbdf" <- function( s, col ) s[ ,col ]
"$<-.dbdf" <- function( s, name, value ) { s[ ,name ] <- value; s }
"[[<-.dbdf" <- function( s, row, col, value ) { s[ row, col ] <- value; s }
"row.names<-.dbdf" <- function( s, value ) { s[ ,"row_names" ] <- value; s }
"names<-.dbdf" <- function( s, value ) stop( "Not supported for dbdf's" )

##Ops.dbdf <- function( e1, e2=NULL ) { print(e1); print(e2); Ops.array( as.matrix( e1 ), e2 ) }

## Should handle s[3,] s[-3,] s[1:3,] s[-(1:3),] s['3',] s[c('1','2','3'),] s[1:3,1:3]
##               s[,3] s[,-3] s[,1:3] s[,-(1:3)] s[,'3'] s[,c('1','2','3')] and all combinations thereof
## TODO DONE: doesnt work correctly if there are duplicates in the input row's or col's
## TODO DONE: accept "row" as a 2-d matrix and if so return a vector of values at the coords in the matrix
"[.dbdf" <- function( s, row, col, drop=T ) {
  if ( missing( row ) && missing( col ) ) { ## s[,] or s[] returns s as a normal (in-mem) data frame
    con <- open.con( s ); on.exit( close.con( con ) )
    cname <- if ( ! is.dbv( s ) ) "*" else paste( "row_idx", "row_names", attr( s, "cname" ), sep="," )
    return( reshape.dbdf.out( my.query( con, paste( "select", cname, "from", attr( s, "tname" ) ) ), s ) )
  }
  if ( is.dbv( s ) && ( missing( col ) || col != "row_names" ) ) col <- attr( s, "cname" )
  row.str <- ""; col.str <- "*"
  con <- open.con( s ); on.exit( close.con( con ) )
  ##dbBeginTransaction( con ); on.exit( { dbCommit( con ); close.con( con ) } )
  if ( ! missing( row ) ) {
    if ( is.character( row ) ) {
      if ( length( row ) > 1 ) row.str <- paste( "where row_names in ('", paste( row, collapse="','" ),
                                                "')", sep="" )
      else row.str <- paste( "where row_names='", row, "'", sep="" )
    } else { ## numeric or integer or matrix of logical...
      if ( is.matrix( row ) && ncol( row ) == 2 && missing( col ) ) { ## .. 2-column matrix
        dbBeginTransaction( con ); on.exit( { dbCommit( con ); close.con( con ) } )
        out <- NULL
        for ( i in unique( row[ ,2 ] ) ) {
          tmp.row <- row[ row[, 2 ] == i, 1 ]
          tmp <- s[ tmp.row, i ]
          if ( is.null( out ) ) out <- vector( typeof( tmp ), nrow( row ) )
          out[ row[, 2 ] == i ] <- tmp
        }
        return( out )
      }
      if ( is.logical( row ) ) row <- which( row )
      else if ( all( row < 0 ) ) row <- ( 1:nrow( s ) )[ row ]
      if ( length( row ) > 1 ) {
        if ( length( unique( row ) ) == diff( range( row ) ) + 1 &&
            all( seq.int( min( row ), max( row ) ) %in% row ) ) { ## It's sequential - faster to use BETWEEN (?)
          row.str <- paste( "where row_idx between", min( row ), "and", max( row ) )
        } else {
          row.str <- paste( "where row_idx in (", paste( sort( unique( row ) ), collapse="," ), ")", sep="" )
        }
      } else if ( length( row ) == 1 ) {
        row.str <- paste( "where row_idx=", row, sep="" )
      } else if ( length( row ) <= 0 ) return( data.frame() )
    }
  }
  if ( ! missing( col ) ) {
    orig.col <- col
    mapping <- get.colname.mapping( s, con )
    if ( ! is.character( col ) ) { ## numeric or integer or logical
      if ( is.logical( col ) ) col <- which( col )
      else if ( all( col < 0 ) ) col <- ( 1:ncol( s ) )[ col ]
      col <- orig.col <- names( mapping )[ col ]
    }
    col <- c( "row_idx", "row_names", mapping[ col ] )
    if ( driver( s ) == "SQLite" ) col.str <- paste( paste( "[", col, "]", sep="" ), collapse="," )
    else col.str <- paste( col, collapse="," )
  } else {
    orig.col <- names( s )
  }
  cmd <- paste( "select", col.str, "from", attr( s, "tname" ), row.str )
  ## Split up # rows if sql command is too big: tested and about 2mb-long command seems to be SQLite limit
  ## Actually command limit is 1,000,000 bytes (http://www.sqlite.org/limits.html) but can be increased.
  if ( nchar( cmd ) > 1000000 && ! missing( row ) ) {
    spl <- round( length( row ) / 2 )
    out <- rbind( s[ row[ 1:spl ], orig.col, drop=F ], s[ row[ (spl+1):length( row ) ], orig.col, drop=F ] )
    return( out )
  }
  out <- my.query( con, cmd )
  ## Reorder output to same ordering as input rows (this is probably slow - what is a better way???):
  ##resort.inds <- NULL; if ( ! missing( row ) ) resort.inds <- out$row_idx[ row ]
  if ( ! missing( row ) && is.numeric( row ) ) {
    rownames( out ) <- make.unique( as.character( out$row_idx ) ); out <- out[ as.character( row ), ] }
  if ( is.null( out ) || sum( dim( out ) ) == 0 ) return( out ) 
  if ( nrow( out ) > 1 && ncol( out ) > 1 ) {
    rownames( out ) <- make.unique( out$row_names )
    if ( ! missing( row ) && ! is.numeric( row ) ) out <- out[ row, ]
  }
  ##if ( length( resort.inds ) > 1 ) out <- out[ resort.inds, ,drop=F ]
  out <- out[ ,-(1:2), drop=F ]
  colnames( out ) <- orig.col
  if ( drop ) {
    if ( ncol( out ) == 1 ) { rnames <- make.unique( rownames( out ) ); out <- out[ ,1 ]; names( out ) <- rnames }
    else if ( nrow( out ) == 1 ) { cnames <- colnames( out ); out <- out[ 1, ]; names( out ) <- cnames }
  }
  ##dbCommit( con )
  out
}

## TODO (DONE): use dbGetPreparedQuery instead of loop, e.g.:
## dbGetPreparedQuery(con,"update iris set Species=:spec where row_idx=:idx",data.frame(spec=150:1,idx=1:150))
## Now it looks like things get "recycled" correctly the same as with a real data.frame!
"[<-.dbdf" <- function( s, row, col, value ) {
  if ( attr( s, "rw" ) == "r" ) stop( "dbdf is read.only (see allow.write())" )

  set.dbdf2 <- function( s, row, col, value ) {
    if ( is.vector( row ) ) row <- t( t( row ) )
    if ( is.vector( value ) ) value <- t( t( value ) )
    colnames( row ) <- paste( "R", 1:ncol( row ), sep="" )
    colnames( value ) <- paste( "V", 1:ncol( value ), sep="" )
    cmd <- paste( "update ", attr( s, "tname" ), " SET ",
                 paste( cnm[ col ], colnames( value ), collapse=",", sep="=:" ), sep="" )
    if ( is.numeric( row ) ) cmd <- paste( cmd, " where row_idx=:", colnames( row ), sep="" )
    else cmd <- paste( cmd, " where row_names=':", colnames( row ), "'", sep="" )
    query.df <- data.frame( row, value )
    if ( dbdf.verbose() ) cat( "DBDF: Prepared query:", cmd, "\n" )
    dbGetPreparedQuery( con, cmd, query.df )
  }    

  con <- open.con( s ); on.exit( close.con( con ) )
  ##dbBeginTransaction( con ); on.exit( { dbCommit( con ); close.con( con ) } )
  cnm <- get.colname.mapping( s, con )
  cnm <- c( cnm, row_names="row_names" ) ## Allow special case where user is changing rownames of s

  if ( is.matrix( row ) && ncol( row ) == 2 && missing( col ) ) { ## .. 2-column matrix input as coords
    dbBeginTransaction( con ); on.exit( { dbCommit( con ); close.con( con ) } )
    tmp <- cbind( row, value )
    for ( i in unique( row[ ,2 ] ) ) {
      whch <- tmp[ tmp[ ,2 ] == i, 1 ]
      s[ tmp[ whch, 1 ], i ] <- tmp[ whch, 3 ]
    }
    return( s )
  }
  
  if ( is.dbv( s ) && col != "row_names" ) col <- attr( s, "cname" )
  if ( missing( row ) ) row <- 1:nrow( s )
  if ( missing( col ) ) col <- 1:ncol( s )
  if ( is.logical( row ) ) row <- which( row )
  if ( is.logical( col ) ) col <- which( col )
  set.dbdf2( s, row, col, value )
  ##dbCommit( con )
  s
}

## delete the dbdf from the workspace(environment) and also delete (drop) its tables.
## If it's in a file db and there are no more tables in that db file, then delete the file
rm.dbdf <- function( s ) {
  if ( attr( s, "rw" ) == "r" ) stop( "dbdf is read.only (see allow.write())" )
  con <- open.con( s ); on.exit( close.con( con ) )
  my.query( con, paste( "drop table", attr( s, "tname" ) ) )
  my.query( con, paste( "drop table ", attr( s, "tname" ), "_colname_map", sep="" ) )
  if ( dbExistsTable( con, paste( attr( s, "tname" ), "_meta_info", sep="" ) ) )
    my.query( con, paste( "drop table ", attr( s, "tname" ), "_meta_info", sep="" ) )
  if ( driver( s ) == "SQLite" && length( dbListTables( con ) ) <= 0 && attr( s, "filename" ) != ":memory:" )
    try( file.remove( attr( s, "filename" ) ) )
  rm( list=deparse( substitute( s ) ), envir=sys.frame( sys.parent() ) )
}

rbind.dbdf <- function( s, df, filename=attr( s, "filename" ), tname=attr( s, "tname" ), chunk.size=100 ) {
  ## Copy s to a new table and append the rows of df to that new table and return a new object
  ## df can be a data.frame or a dbdf
  con <- open.con( filename, driver( s ) ); on.exit( close.con( con ) )
  new.s <- dbdf( s, filename, tname, chunk.size )

  if ( ! is.dbdf( df ) ) chunk.size <- nrow( df )
  ch.inds <- c( seq( 1, nrow( df ), by=chunk.size ), nrow( df ) + 1 )

  for ( i in 1:( length( ch.inds ) - 1 ) ) {
    tmp <- df[ ch.inds[ i ]:( ch.inds[ i+1 ] - 1 ), ]
    tmp <- cbind( row_idx=ch.inds[ i ]:( ch.inds[ i+1 ] - 1 ) + nrow( s ), row_names=rownames( tmp ), tmp )
    if ( dbdf.verbose() ) cat( "DBDF: Appending", nrow( tmp ), "rows to table", tname, "in file", filename, "\n" )
    dbWriteTable( con, attr( new.s, "tname" ), tmp, row.names=F, append=T )
  }
  attr( new.s, "nrow" ) <- attr( s, "nrow" ) + nrow( df )
  if ( driver( s ) == "MySQL" ) attr( new.s, "dbname" ) <- dbGetInfo( con )$dbname ## Could be different
  new.s
}

cbind.dbdf <- function( s, df, filename=attr( s, "filename" ), tname=attr( s, "tname" ), chunk.size=100 ) {
  ## Append columns of dataframe (or dbdf) df to the end of table for s and return new dbdf object.
  ## Do it by fetching s as a data.frame in chunks, cbind it with corresponding chunks of df, and append
  ##   the cbinded data.frame to a new table; then replace the old table with the new one in the output dbdf.
  if ( ! is.data.frame( df ) ) {
    df.name <- deparse( substitute( df ) )
    is.vec <- is.vector( df )
    df <- as.data.frame( df )
    if ( is.vec ) colnames( df ) <- df.name
  }
  ch.inds <- c( seq( 1, nrow( s ), by=chunk.size ), nrow( s ) + 1 )
  new.s <- NULL
  for ( i in 1:( length( ch.inds ) - 1 ) ) {
    tmp <- cbind( s[ ch.inds[ i ]:( ch.inds[ i+1 ] - 1 ), ], df[ ch.inds[ i ]:( ch.inds[ i+1 ] - 1 ), ] )
    colnames( tmp )[ ( ncol( s ) + 1 ):ncol( tmp ) ] <- colnames( df )
    if ( is.null( new.s ) ) new.s <- dbdf( tmp, filename=filename, tname=tname )
    else new.s <- rbind.dbdf( new.s, tmp )
  }
  new.s
}

apply.dbdf <- function( s, MAR, FUN, chunk.size=100, ... ) {
  FUN <- match.fun( FUN )
  out <- NULL
  con <- open.con( s ) ##; on.exit( close.con( con ) ) ## Keep it from being re-opened mult times
  ##dbBeginTransaction( con ); on.exit( { dbCommit( con ); close.con( con ) } )
  if ( MAR == 1 ) {
    ch.inds <- c( seq( 1, nrow( s ), by=chunk.size ), nrow( s ) + 1 )
    for ( i in 1:( length( ch.inds ) - 1 ) ) {
      tmp <- apply( s[ ch.inds[ i ]:( ch.inds[ i+1 ] - 1 ), ], 1, FUN, ... )
      if ( is.vector( tmp ) ) out <- c( out, tmp )
      else if ( is.matrix( tmp ) ) out <- rbind( out, tmp )
    }
  } else if ( MAR == 2 ) {
    for ( i in names( s ) ) {
      tmp <- FUN( s[ ,i ], ... )
      if ( is.vector( tmp ) ) out <- c( out, tmp )
      else if ( is.matrix( tmp ) ) out <- rbind( out, tmp )
    }
  }
  ##dbCommit( con )
  out
}

## Examples using iris data:
## THIS WORKS: x <- 'virginica'; select( s, select='*', where="Species='$x'", limit=3 )
## THIS WORKS: x <- c(5,6); select( s, select='*', where="Sepal_Length between `x[1]` and `x[2]`", limit=3 )
## THIS WORKS: x <- c('virginica','setosa');  select( s, select='*', where="Species in $x", limit=3 )
## THIS WORKS: y <- c('Species','Petal.Width'); select( s, select="$y", where="Species in $x", limit=3 )
## Note: "subset.data.frame" does the same thing but on data frames - see below for dbdf function

select <- function( s, ... ) UseMethod( "select" )

select.dbdf <- function( s, select="*", where="", limit="", order="", group.by="" ) {
  require( gsubfn ) ## allows, e.g. gsubfn( , , 'pi = $pi, exp = `exp(1)`\n' ) to return
  ##                        'pi = 3.14159265358979, exp = 2.71828182845905'
  if ( is.dbv( s ) ) select <- paste( "row_idx", "row_names", attr( s, "cname" ), sep="," )
  else {
    select <- gsubfn( , , select, env=sys.frame( sys.parent() ) )
    select <- gsub( 'c\\(', '', select, perl=T )
    select <- gsub( '\\)', '', select, perl=T )
  }
  con <- open.con( s ); on.exit( close.con( con ) ) ## Keep it from being re-opened mult times
  ##dbBeginTransaction( con ); on.exit( { dbCommit( con ); close.con( con ) } )
  mapping <- get.colname.mapping( s, con )
  for ( m in names( mapping ) ) {
    select <- gsub( m, mapping[ m ], select, fixed=T )
    where <- gsub( m, mapping[ m ], where, fixed=T )
    limit <- gsub( m, mapping[ m ], limit, fixed=T )
    order <- gsub( m, mapping[ m ], order, fixed=T )
    group.by <- gsub( m, mapping[ m ], group.by, fixed=T )
  }
  where <- if ( where != "" ) paste( "where", gsubfn( , , where, env=sys.frame( sys.parent() ) ) ) else ""
  limit <- if ( limit != "" ) paste( "limit",
                                    gsubfn( , , as.character( limit ), env=sys.frame( sys.parent() ) ) ) else ""
  order <- if ( order != "" ) paste( "order by", 
                                    gsubfn( , , as.character( order ), env=sys.frame( sys.parent() ) ) ) else ""
  group.by <- if ( group.by != "" ) paste( "group by", 
                                    gsubfn( , , as.character( group.by ), env=sys.frame( sys.parent() ) ) ) else ""
  if ( driver( s ) == "SQLite" ) cmd <- paste( "select ", select, " from [", attr( s, "tname" ), "] ",
                    where, " ", limit, " ", order, " ", group.by, sep="" )
  else cmd <- paste( "select ", select, " from ", attr( s, "tname" ), " ", where, " ", limit, " ", order, " ",
                    group.by, sep="" )
  cmd <- gsub( 'c(', '(', cmd, fixed=T )
  out <- my.query( con, cmd )
  if ( is.null( out ) || sum( dim( out ) ) == 0 ) return( out )
  if ( 'row_idx' %in% colnames( out ) && ! 'row_idx' %in% select ) {
    out <- out[ ,-which( colnames( out ) == 'row_idx' ) ]
  }
  if ( 'row_names' %in% colnames( out ) && ! 'row_names' %in% select ) {
    rownames( out ) <- make.unique( out$row_names )
    out <- out[ ,-which( colnames( out ) == 'row_names' ) ]
  }
  for ( m in mapping ) colnames( out )[ colnames( out ) == m ] <- names( mapping )[ mapping == m ]
  ##dbCommit( con )
  out
}

## See docs for subset() and code for subset.data.frame() ... similar to select() above
## Doesn't implement all subset() functionality but simple things like this work:
##     subset(s,Species%in%c('virginica','versicolor')|Sepal.Length<=3,c(Species,Sepal.Length))
## And can use limited substitution stuff from gsubfn(), e.g. (note the quotes around '$x' is required):
##     x.1<-5; subset(s,Species%in%c('virginica','versicolor')|Sepal.Length<='$x.1',c(Species,Sepal.Length))
## Or something like this:
##     subset(s,Species%in%c('virginica','versicolor')|Sepal.Length<=`exp(2)`,c(Species,Sepal.Length))
## Or even:
##     subset(s,Species%in%c('virginica','versicolor')|Sepal.Length<=`get('x')`,c(Species,Sepal.Length))
subset.dbdf <- function( s, subset, select, drop=F, ... ) {
  require( gsubfn )
  e <- ""
  cnm <- colnames( s )
  tmp <- 1:length( cnm ); names( tmp ) <- cnm; tmp <- as.list( tmp ); attach( tmp ); on.exit( detach( tmp ) )
  if ( ! missing( subset ) ) {
    e <- paste( deparse( substitute( subset ) ), collapse=" " )
    e <- gsub( ", ", ",", gsub( "\\|", "OR", gsub( "\\|\\|", "OR", gsub( "\\&", "AND",
              gsub( "\\&\\&", "AND", gsub( "%in%", " in ", gsub( "==", "=", gsub( '\\"', "'", e ) ) ) ) ) ) ) )
    ##e <- gsub( "[\"'](\\$\\S+)[\"']", "\\1", e, perl=T )
    ##e <- gsub( "^'", "", gsub( "'$", "", e, perl=T ), perl=T )
    e <- gsub( "^[\"'](.*)[\"']$", "\\1", e, perl=T )
    for ( i in cnm ) e <- gsub( paste( "`", i, "`", sep="" ), i, e )
    if ( driver( s ) == "SQLite" ) for ( i in cnm ) e <- gsub( i, paste( "[", i, "]", sep="" ), e )
    e <- gsubfn( , , e, env=sys.frame( sys.parent() ) )
  }

  e2 <- "*"
  if ( ! missing( select ) ) {
    e2 <- deparse( substitute( select ) )
    e2 <- gsub( ", ", ",", gsub( "\\)$", "", gsub( "c\\(", "", e2 ), perl=T ) )
    for ( i in cnm ) e <- gsub( paste( "`", i, "`", sep="" ), i, e )
    if ( driver( s ) == "SQLite" ) for ( i in cnm ) e2 <- gsub( i, paste( "[", i, "]", sep="" ), e2 )
    e2 <- gsubfn( , , e2, env=sys.frame( sys.parent() ) )
  }

  out <- select.dbdf( s, select=e2, where=e )
  if ( drop && ncol( out ) == 1 ) out <- out[ ,1 ]
  out
}

## Create a merge SQL query. For now, "all", "suffixes", and "incomparables" are ignored;
##   x and y have to be dbdf's that are stored in the SAME DATABASE FILE!!!
##   NOTE this returns a data.frame (not a new dbdf!); out.dbdf=T will eventually change that but not quite there yet
merge.dbdf <- function( x, y, by=intersect(names(x), names(y)),
                       by.x=by, by.y=by, all=FALSE, all.x=all, all.y=all,
                       sort=TRUE, suffixes=c(".x",".y"), incomparables=NULL, out.dbdf=F, ... ) {
  f1 <- attr( x, "filename" )
  f2 <- attr( y, "filename" )
  if ( f1 != f2 ) stop( paste( "Different database files currently not allowed:", f1, f2 ) )
  t1 <- attr( x, "tname" )
  t2 <- attr( y, "tname" )
  if ( ! out.dbdf ) query <- paste( 'select * from ', t1, ',', t2, sep="" )
  if ( length( by.x ) > 0 ) {
    query <- paste( query, ' where ', t1, '.', by.x, '=', t2, '.', by.y, sep="" )
    if ( sort ) query <- paste( query, ' order by ', t1, '.', by.x, ',', t2, '.', by.y, sep="" )
  }
  if ( out.dbdf ) query <- paste( 'create table merged_', t1, '_', t2, ' as ', query, sep="" )
  con <- open.con( x ); on.exit( close.con( con ) )
  out <- my.query( con, query )
  if ( ! out.dbdf ) {
    rownames( out ) <- make.unique( out$row_names )
    out <- out[ , ! colnames( out ) %in% c( 'row_idx', 'row_names' ) ]
    out <- out[ , colnames( out ) != make.unique( c( by.x, by.y ) )[ 2 ] ]
    return( out )
  } else {
    ## Ideally take the new table and make a dbdf object out of it. Also need to make the
    ##   colname-mapping table for it and remove the second row_names and row_idx columns
    ##   (saved as 'row_names:1' and 'row_idx:1' in the database)
  }
}

## Recursively convert a list to a set of dbdfs only if the element's object.size() is > 2048 bytes
as.dbdf.list <- function( l, filename=NULL, tname=NULL, overwrite=T, size.dont.bother=2048, driver=dbdf.driver() ) {
  if ( is.null( filename ) ) {
    ##if ( driver == "SQLite" )
    ##  filename <- paste( ".dbdf/", DBI:::make.db.names.default( deparse( substitute( l ) ) ), ".db", sep="" )
    ##else filename <- DBI:::make.db.names.default( deparse( substitute( l ) ) )
    filename <- sprintf( dbdf.database( driver=driver ), DBI:::make.db.names.default( deparse( substitute( l ) ) ) )
  }
  if ( is.null( tname ) ) tname <- deparse( substitute( l ) )
  if ( object.size( l ) <= size.dont.bother ) return( l )
  con <- open.con( filename, driver ); on.exit( close.con( con ) )
  if ( is.data.frame( l ) ) return( dbdf( l, filename=filename, tname=tname ) )
  else if ( ( is.matrix( l ) || is.vector( l ) ) && ! "list" %in% class( l ) )
    return( dbdf( as.data.frame( l ), filename=filename, tname=tname ) )
  else if ( all( sapply( l, is.vector ) ) && all( sapply( l, function( i ) ! "list" %in% class( i ) ) ) &&
           all( sapply( l, length ) == length( l[[ 1 ]] ) ) )
    return( dbdf( as.data.frame( l ), filename, tname=tname ) )
  return( lapply( l, as.dbdf.list, filename=filename, tname=tname, overwrite=F,
                 size.dont.bother=size.dont.bother ) )    
}

ls.dbdf <- function() names( which( sapply( sapply( ls( env=parent.frame() ), get ), is.dbdf ) ) )

## Fetch an dbdf from the database w/ given name; if tname is NULL get all dbdfs from all tables in the
## DB file as a list; if fname is null get all dbs from given database(mysql) or directory(sqlite)
get.dbdf <- function( fname, tname=NULL, driver=dbdf.driver() ) {
  if ( missing( fname ) || is.null( fname ) ) {
    if ( driver == "SQLite" ) {
      fname <- dirname( sprintf( dbdf.database( driver=driver ), "crap" ) )
      fname <- list.files( path=fname, pattern='.db', full=T )
    } else {
      fname <- dbdf.database( driver=driver )
    }
  } else if ( driver == "SQLite" && ! file.exists( fname ) ) {
    fname <- sprintf( dbdf.database( driver=driver ), DBI:::make.db.names.default( fname ) )
    if ( ! file.exists( fname ) ) stop( paste( fname, "doesn't exist" ) )
  }

  out <- list()
  for ( f in fname ) {
    if ( is.null( tname ) ) {
      con <- open.con( f, driver )
      tn <- dbListTables( con )
      close.con( con )
      tn <- tn[ -grep( "_colname_map$", tn, perl=T ) ]
    } else {
      tn <- tname
    }
    out[[ f ]] <- lapply( tn, function( i ) {
      ##dbdf( NULL, f, tn )
      out <- list()
      attr( out, "class" ) <- c( "dbdf", "data.frame" )
      attr( out, "filename" ) <- f
      attr( out, "tname" ) <- i
      attr( out, "nrow" ) <- length( rownames( out ) )
      attr( out, "ncol" ) <- length( colnames( out ) )
      attr( out, "driver" ) <- driver
      if ( driver == "MySQL" ) attr( out, "dbname" ) <- dbGetInfo( con )$dbname ## Could be different
      out
    } )
    names( out[[ f ]] ) <- tn
  }
  if ( length( out ) == 1 ) out <- out[[ 1 ]]
  else if ( length( out ) < 1 ) out <- NULL
  out
}

################################ WORK IN PROGRESS ############################

## dbv: a database-stored vector. Either a new 1-column table (dbv) or a column in an already-existing dbdf
##    table (dbv2). For second case, various dbdf functions (above) have been modified.
dbv <- function( df, filename=NULL, overwrite=F, index="default", tname=NULL, chunk.size=100,
                read.func=read.delim, ... ) {
  if ( ! is.vector( df ) ) stop( "Try dbdf instead" )
  v <- dbdf( df, filename, overwrite, index, tname, chunks, read.func, ... )
  attr( v, "class" ) <- c( "dbv", "dbdf", "vector", "data.frame" )
  attr( v, "cname" ) <- "df"
  v
}

dbv2 <- function( s, column ) {
  if ( ! column %in% colnames( s ) ) stop( "Invalid column name" )
  v <- s
  attr( v, "class" ) <- c( "dbv", "dbdf", "vector", "data.frame" )
  attr( v, "cname" ) <- column
  v
}

length.dbv <- function( v ) nrow( as.dbdf( v ) )
names.dbv <- function( v ) dimnames( v )[[ 1 ]]
"names<-.dbv" <- function( v, value ) { rownames( v ) <- value; v }
as.vector.dbv <- function( v, mode="any" ) as.data.frame( v )

as.dbdf <- function( v ) UseMethod( "as.dbdf" )
as.dbdf.dbv <- function( v ) { attr( v, "class" ) <- c( "dbdf", "data.frame" ); v }
##c.dbv <- function( v1, v2 ) rbind.dbdf( v1, v2 )
