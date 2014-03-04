DATE <-
"Tue Mar  4 11:10:38 2014"
VERSION <-
"0.0.4"
.onLoad <-
function( libname, pkgname ) { ##.onAttach
    cat( "Loading ", pkgname, " version ", VERSION, " (", DATE, ")\n", sep="" )
    cat( "Copyright (C) David J Reiss, Institute for Systems Biology.\n" )
    cat( "Please email dreiss@systemsbiology.org if you run into any issues.\n" )
  }
allow.write <-
function (s, rw = T) 
{
    attr(s, "rw") <- if (rw) 
        "rw"
    else "r"
    s
}
apply.dbdf <-
function (s, MAR, FUN, chunk.size = 100, ...) 
{
    FUN <- match.fun(FUN)
    out <- NULL
    con <- open.con(s)
    if (MAR == 1) {
        ch.inds <- c(seq(1, nrow(s), by = chunk.size), nrow(s) + 
            1)
        for (i in 1:(length(ch.inds) - 1)) {
            tmp <- apply(s[ch.inds[i]:(ch.inds[i + 1] - 1), ], 
                1, FUN, ...)
            if (is.vector(tmp)) 
                out <- c(out, tmp)
            else if (is.matrix(tmp)) 
                out <- rbind(out, tmp)
        }
    }
    else if (MAR == 2) {
        for (i in names(s)) {
            tmp <- FUN(s[, i], ...)
            if (is.vector(tmp)) 
                out <- c(out, tmp)
            else if (is.matrix(tmp)) 
                out <- rbind(out, tmp)
        }
    }
    out
}
as.data.frame.dbdf <-
function (s, row.names = NULL, optional = FALSE, ...) 
s[]
as.dbdf <-
function (v) 
UseMethod("as.dbdf")
as.dbdf.dbv <-
function (v) 
{
    attr(v, "class") <- c("dbdf", "data.frame")
    v
}
as.dbdf.list <-
function (l, filename = NULL, tname = NULL, overwrite = T, size.dont.bother = 2048, 
    driver = dbdf.driver()) 
{
    if (is.null(filename)) {
        filename <- sprintf(dbdf.database(driver = driver), DBI:::make.db.names.default(deparse(substitute(l))))
    }
    if (is.null(tname)) 
        tname <- deparse(substitute(l))
    if (object.size(l) <= size.dont.bother) 
        return(l)
    con <- open.con(filename, driver)
    on.exit(close.con(con))
    if (is.data.frame(l)) 
        return(dbdf(l, filename = filename, tname = tname))
    else if ((is.matrix(l) || is.vector(l)) && !"list" %in% class(l)) 
        return(dbdf(as.data.frame(l), filename = filename, tname = tname))
    else if (all(sapply(l, is.vector)) && all(sapply(l, function(i) !"list" %in% 
        class(i))) && all(sapply(l, length) == length(l[[1]]))) 
        return(dbdf(as.data.frame(l), filename, tname = tname))
    return(lapply(l, as.dbdf.list, filename = filename, tname = tname, 
        overwrite = F, size.dont.bother = size.dont.bother))
}
as.list.dbdf <-
function (s) 
as.list(as.data.frame(s))
as.matrix.dbdf <-
function (s) 
as.matrix(as.data.frame(s))
as.vector.dbdf <-
function (s, mode = "any") 
as.vector(as.matrix(s), mode = mode)
as.vector.dbv <-
function (v, mode = "any") 
as.data.frame(v)
cbind.dbdf <-
function (s, df, filename = attr(s, "filename"), tname = attr(s, 
    "tname"), chunk.size = 100) 
{
    if (!is.data.frame(df)) {
        df.name <- deparse(substitute(df))
        is.vec <- is.vector(df)
        df <- as.data.frame(df)
        if (is.vec) 
            colnames(df) <- df.name
    }
    ch.inds <- c(seq(1, nrow(s), by = chunk.size), nrow(s) + 
        1)
    new.s <- NULL
    for (i in 1:(length(ch.inds) - 1)) {
        tmp <- cbind(s[ch.inds[i]:(ch.inds[i + 1] - 1), ], df[ch.inds[i]:(ch.inds[i + 
            1] - 1), ])
        colnames(tmp)[(ncol(s) + 1):ncol(tmp)] <- colnames(df)
        if (is.null(new.s)) 
            new.s <- dbdf(tmp, filename = filename, tname = tname)
        else new.s <- rbind.dbdf(new.s, tmp)
    }
    new.s
}
close.con <-
function (con, force = F) 
{
    try({
        open.rs <- dbGetInfo(con)$rsId
        if (length(open.rs) > 0) 
            sapply(open.rs, dbClearResult)
    })
    if (force || !attr(con, "not.new")) {
        if (dbdf.verbose()) 
            cat("DBDF: Closing", attr(class(con), "package"), 
                "connection to:", dbGetInfo(con)$dbname, "\n")
        dbDisconnect(con)
    }
}
dbdf <-
function (df, filename = NULL, overwrite = F, index = "default", 
    tname = NULL, chunk.size = 100, read.func = read.delim, rw = "r", 
    driver = dbdf.driver(), ...) 
{
    if (missing(df) || is.null(df)) {
        out <- list()
        attr(out, "class") <- c("dbdf", "data.frame")
        attr(out, "filename") <- filename
        attr(out, "tname") <- tname
        con <- open.con(filename, driver)
        on.exit(close.con(con))
        tmp <- my.query(con, paste("select * from", attr(out, 
            "tname"), "limit 1"))
        tmp <- tmp[, !colnames(tmp) %in% c("row_idx", "row_names")]
        attr(out, "ncol") <- ncol(tmp)
        attr(out, "nrow") <- my.query(con, paste("select count(*) from", 
            tname))[, 1]
        attr(out, "rw") <- rw
        attr(out, "driver") <- driver
        if (dbdf.meta()) 
            write.default.meta.info(out, "create")
        return(out)
    }
    else if ((is.character(df) && file.exists(df)) || "connection" %in% 
        class(df)) {
        orig.con <- NULL
        if (!"connection" %in% class(df)) {
            if (is.null(filename)) {
                filename <- sprintf(dbdf.database(driver = driver), 
                  DBI:::make.db.names.default(df))
            }
            if (is.null(tname)) 
                tname <- DBI:::make.db.names.default(df)
        }
        else {
            filename <- sprintf(dbdf.database(driver = driver), 
                DBI:::make.db.names.default(summary(df)$description))
            tname <- DBI:::make.db.names.default(summary(df)$description)
            orig.con <- df
        }
        filename <- gsub("[\"']", "", filename)
        tname <- gsub("[\"']", "", tname)
        con <- open.con(filename, driver)
        on.exit(close.con(con))
        new.df <- tmp <- NULL
        skip <- 0
        while (is.null(tmp) || nrow(tmp) == chunk.size) {
            if (!is.null(orig.con)) 
                df <- orig.con
            tmp <- read.func(df, skip = skip, nrow = chunk.size, 
                header = (skip == 0), ...)
            if (skip == 0) {
                cnames <- colnames(tmp)
                new.dbdf <- dbdf(tmp, filename, overwrite = T, 
                  index = index, tname = tname, ...)
            }
            else {
                colnames(tmp) <- cnames
                if (all(rownames(tmp) == as.character(1:nrow(tmp)))) 
                  rownames(tmp) <- skip:(skip + nrow(tmp) - 1) + 
                    1
                tmp <- cbind(row_idx = skip:(skip + nrow(tmp) - 
                  1) + 1, row_names = rownames(tmp), tmp)
                if (dbdf.verbose()) 
                  cat("DBDF: Appending", nrow(tmp), "rows to table", 
                    tname, "in file", filename, "\n")
                dbWriteTable(con, tname, tmp, row.names = F, 
                  append = T)
            }
            skip <- skip + nrow(tmp)
        }
        attr(new.dbdf, "nrow") <- skip + 1
        if (driver == "MySQL") 
            attr(new.dbdf, "dbname") <- dbGetInfo(con)$dbname
        if (dbdf.meta()) 
            write.default.meta.info(new.dbdf, "create")
        return(new.dbdf)
    }
    else if (is.dbdf(df)) {
        if (is.null(filename)) 
            filename <- attr(df, "filename")
        if (is.null(tname)) 
            tname <- attr(df, "tname")
        chunk.size <- min(nrow(df), chunk.size)
        out <- dbdf(as.data.frame(df[1:chunk.size, ]), filename = filename, 
            tname = tname, driver = driver, ...)
        if (nrow(out) == nrow(df)) 
            return(out)
        filename <- attr(out, "filename")
        tname <- attr(out, "tname")
        con <- open.con(filename, driver)
        on.exit(close.con(con))
        ch.inds <- c(seq(1, nrow(df), by = chunk.size), nrow(df) + 
            1)
        for (i in 2:(length(ch.inds) - 1)) {
            tmp <- as.data.frame(df[ch.inds[i]:(ch.inds[i + 1] - 
                1), ])
            tmp <- cbind(row_idx = ch.inds[i]:(ch.inds[i + 1] - 
                1), row_names = rownames(tmp), tmp)
            if (dbdf.verbose()) 
                cat("DBDF: Appending", nrow(tmp), "rows to table", 
                  tname, "in file", filename, "\n")
            dbWriteTable(con, tname, tmp, row.names = F, append = T)
        }
        attr(out, "nrow") <- my.query(con, paste("select count(row_idx) from", 
            attr(out, "tname")))[, 1]
        tmp <- my.query(con, paste("select * from", attr(out, 
            "tname"), "limit 1"))
        tmp <- tmp[, !colnames(tmp) %in% c("row_idx", "row_names")]
        attr(out, "ncol") <- ncol(tmp)
        attr(out, "driver") <- driver
        if (driver == "MySQL") 
            attr(out, "dbname") <- dbGetInfo(con)$dbname
        if (dbdf.meta()) 
            write.default.meta.info(out, "create")
        return(out)
    }
    else if (is.null(filename)) {
        filename <- sprintf(dbdf.database(driver = driver), DBI:::make.db.names.default(deparse(substitute(df))))
    }
    if (is.null(tname)) 
        tname <- DBI:::make.db.names.default(deparse(substitute(df)))
    else tname <- DBI:::make.db.names.default(tname)
    if (!is.data.frame(df)) 
        df <- as.data.frame(df)
    out <- list()
    attr(out, "class") <- c("dbdf", "data.frame")
    attr(out, "filename") <- filename
    attr(out, "tname") <- tname
    attr(out, "nrow") <- nrow(df)
    attr(out, "ncol") <- ncol(df)
    attr(out, "rw") <- rw
    attr(out, "driver") <- driver
    con <- open.con(filename, driver)
    on.exit(close.con(con))
    if (driver == "MySQL") {
        attr(out, "dbname") <- dbGetInfo(con)$dbname
        colnames(df) <- make.unique(substr(colnames(df), 1, 64))
    }
    t.exists <- dbExistsTable(con, tname)
    if (t.exists && !overwrite) {
        orig.tname <- tname
        ind <- 1
        while (dbExistsTable(con, tname)) {
            tname <- paste(orig.tname, "_", ind, sep = "")
            ind <- ind + 1
        }
        attr(out, "tname") <- tname
        t.exists <- FALSE
    }
    if (!t.exists || overwrite) {
        if (dbdf.verbose()) 
            cat("DBDF: Creating", driver, "table", tname, "in DB", 
                filename, "\n")
        if (is.null(rownames(df)) || length(rownames(df)) == 
            0) 
            rownames(df) <- make.unique(as.character(df[, 1]))
        dbWriteTable(con, tname, cbind(row_idx = 1:nrow(df), 
            row_names = rownames(df), df), row.names = F, overwrite = T)
        new.colnames <- my.query(con, paste("select * from", 
            tname, "limit 1"))
        new.colnames <- colnames(new.colnames)[-(1:2)]
        if (dbdf.verbose()) 
            cat("DBDF: Creating", driver, "table", paste(tname, 
                "colname_map", sep = "_"), "in DB", filename, 
                "\n")
        dbWriteTable(con, paste(tname, "colname_map", sep = "_"), 
            data.frame(orig = colnames(df), new = new.colnames), 
            row.names = F, overwrite = T)
        idx.columns <- paste(c("row_idx", "row_names"), collapse = ",")
        if (driver == "MySQL") 
            idx.columns <- paste(c("row_idx", paste("row_names(", 
                max(nchar(rownames(df))), ")", sep = "")), collapse = ",")
        if (index[1] == "all") {
            if (driver == "SQLite") 
                new.colnames <- paste("[", new.colnames, "]", 
                  sep = "")
            idx.columns <- paste(c(idx.columns, paste(new.colnames, 
                collapse = ",")), collapse = ",")
        }
        else if (index[1] != "default") {
            new.idx <- new.colnames[colnames(df) %in% index]
            if (driver == "SQLite") 
                new.idx <- paste("[", new.idx, "]", sep = "")
            idx.columns <- paste(idx.columns, c(paste(new.idx, 
                collapse = ",")), sep = ",")
        }
        my.query(con, paste("create index RDB_", tname, "_INDEX on ", 
            tname, "(", idx.columns, ")", sep = ""))
        if (dbdf.meta()) 
            write.default.meta.info(out, "create")
    }
    else {
        if (dbdf.meta()) 
            write.default.meta.info(out, "update")
    }
    out
}
`[<-.dbdf` <-
function (s, row, col, value) 
{
    if (attr(s, "rw") == "r") 
        stop("dbdf is read.only (see allow.write())")
    set.dbdf2 <- function(s, row, col, value) {
        if (is.vector(row)) 
            row <- t(t(row))
        if (is.vector(value)) 
            value <- t(t(value))
        colnames(row) <- paste("R", 1:ncol(row), sep = "")
        colnames(value) <- paste("V", 1:ncol(value), sep = "")
        cmd <- paste("update ", attr(s, "tname"), " SET ", paste(cnm[col], 
            colnames(value), collapse = ",", sep = "=:"), sep = "")
        if (is.numeric(row)) 
            cmd <- paste(cmd, " where row_idx=:", colnames(row), 
                sep = "")
        else cmd <- paste(cmd, " where row_names=':", colnames(row), 
            "'", sep = "")
        query.df <- data.frame(row, value)
        if (dbdf.verbose()) 
            cat("DBDF: Prepared query:", cmd, "\n")
        dbGetPreparedQuery(con, cmd, query.df)
    }
    con <- open.con(s)
    on.exit(close.con(con))
    cnm <- get.colname.mapping(s, con)
    cnm <- c(cnm, row_names = "row_names")
    if (is.matrix(row) && ncol(row) == 2 && missing(col)) {
        dbBeginTransaction(con)
        on.exit({
            dbCommit(con)
            close.con(con)
        })
        tmp <- cbind(row, value)
        for (i in unique(row[, 2])) {
            whch <- tmp[tmp[, 2] == i, 1]
            s[tmp[whch, 1], i] <- tmp[whch, 3]
        }
        return(s)
    }
    if (is.dbv(s) && col != "row_names") 
        col <- attr(s, "cname")
    if (missing(row)) 
        row <- 1:nrow(s)
    if (missing(col)) 
        col <- 1:ncol(s)
    if (is.logical(row)) 
        row <- which(row)
    if (is.logical(col)) 
        col <- which(col)
    set.dbdf2(s, row, col, value)
    s
}
`[.dbdf` <-
function (s, row, col, drop = T) 
{
    if (missing(row) && missing(col)) {
        con <- open.con(s)
        on.exit(close.con(con))
        cname <- if (!is.dbv(s)) 
            "*"
        else paste("row_idx", "row_names", attr(s, "cname"), 
            sep = ",")
        return(reshape.dbdf.out(my.query(con, paste("select", 
            cname, "from", attr(s, "tname"))), s))
    }
    if (is.dbv(s) && (missing(col) || col != "row_names")) 
        col <- attr(s, "cname")
    row.str <- ""
    col.str <- "*"
    con <- open.con(s)
    on.exit(close.con(con))
    if (!missing(row)) {
        if (is.character(row)) {
            if (length(row) > 1) 
                row.str <- paste("where row_names in ('", paste(row, 
                  collapse = "','"), "')", sep = "")
            else row.str <- paste("where row_names='", row, "'", 
                sep = "")
        }
        else {
            if (is.matrix(row) && ncol(row) == 2 && missing(col)) {
                dbBeginTransaction(con)
                on.exit({
                  dbCommit(con)
                  close.con(con)
                })
                out <- NULL
                for (i in unique(row[, 2])) {
                  tmp.row <- row[row[, 2] == i, 1]
                  tmp <- s[tmp.row, i]
                  if (is.null(out)) 
                    out <- vector(typeof(tmp), nrow(row))
                  out[row[, 2] == i] <- tmp
                }
                return(out)
            }
            if (is.logical(row)) 
                row <- which(row)
            else if (all(row < 0)) 
                row <- (1:nrow(s))[row]
            if (length(row) > 1) {
                if (length(unique(row)) == diff(range(row)) + 
                  1 && all(seq.int(min(row), max(row)) %in% row)) {
                  row.str <- paste("where row_idx between", min(row), 
                    "and", max(row))
                }
                else {
                  row.str <- paste("where row_idx in (", paste(sort(unique(row)), 
                    collapse = ","), ")", sep = "")
                }
            }
            else if (length(row) == 1) {
                row.str <- paste("where row_idx=", row, sep = "")
            }
            else if (length(row) <= 0) 
                return(data.frame())
        }
    }
    if (!missing(col)) {
        orig.col <- col
        mapping <- get.colname.mapping(s, con)
        if (!is.character(col)) {
            if (is.logical(col)) 
                col <- which(col)
            else if (all(col < 0)) 
                col <- (1:ncol(s))[col]
            col <- orig.col <- names(mapping)[col]
        }
        col <- c("row_idx", "row_names", mapping[col])
        if (driver(s) == "SQLite") 
            col.str <- paste(paste("[", col, "]", sep = ""), 
                collapse = ",")
        else col.str <- paste(col, collapse = ",")
    }
    else {
        orig.col <- names(s)
    }
    cmd <- paste("select", col.str, "from", attr(s, "tname"), 
        row.str)
    if (nchar(cmd) > 1e+06 && !missing(row)) {
        spl <- round(length(row)/2)
        out <- rbind(s[row[1:spl], orig.col, drop = F], s[row[(spl + 
            1):length(row)], orig.col, drop = F])
        return(out)
    }
    out <- my.query(con, cmd)
    if (!missing(row) && is.numeric(row)) {
        rownames(out) <- make.unique(as.character(out$row_idx))
        out <- out[as.character(row), ]
    }
    if (is.null(out) || sum(dim(out)) == 0) 
        return(out)
    if (nrow(out) > 1 && ncol(out) > 1) {
        rownames(out) <- make.unique(out$row_names)
        if (!missing(row) && !is.numeric(row)) 
            out <- out[row, ]
    }
    out <- out[, -(1:2), drop = F]
    colnames(out) <- orig.col
    if (drop) {
        if (ncol(out) == 1) {
            rnames <- make.unique(rownames(out))
            out <- out[, 1]
            names(out) <- rnames
        }
        else if (nrow(out) == 1) {
            cnames <- colnames(out)
            out <- out[1, ]
            names(out) <- cnames
        }
    }
    out
}
`[[<-.dbdf` <-
function (s, row, col, value) 
{
    s[row, col] <- value
    s
}
`[[.dbdf` <-
function (s, col) 
s[, col]
`$<-.dbdf` <-
function (s, name, value) 
{
    s[, name] <- value
    s
}
`$.dbdf` <-
function (s, name) 
s[[name]]
dbdf.database <-
function (dbname = c(SQLite = ".dbdf/%s.db", MySQL = "dbdf"), 
    driver = dbdf.driver()) 
{
    if (!missing(dbname)) {
        if (driver %in% names(dbname)) 
            options(dbdf.dbname = dbname[driver])
        else options(dbdf.dbname = dbname)
    }
    else if (is.null(options("dbdf.dbname")$dbdf.dbname)) {
        options(dbdf.dbname = dbname[driver])
    }
    options("dbdf.dbname")$dbdf.dbname
}
dbdf.driver <-
function (d = "SQLite") 
{
    if (missing(d)) 
        return(options("dbdf.driver")$dbdf.driver)
    options(dbdf.driver = d)
    if (d == "SQLite") 
        require(RSQLite)
    else if (d == "MySQL") 
        require(RMySQL)
    options("dbdf.driver")$dbdf.driver
}
dbdf.meta <-
function (m) 
{
    if (missing(m)) {
        out <- options("dbdf.meta")$dbdf.meta
        if (is.null(out)) 
            options(dbdf.meta = F)
        return(options("dbdf.meta")$dbdf.meta)
    }
    options(dbdf.meta = m)
}
dbdf.verbose <-
function (v) 
{
    if (missing(v)) {
        out <- options("dbdf.verbose")$dbdf.verbose
        if (is.null(out)) 
            options(dbdf.verbose = F)
        return(options("dbdf.verbose")$dbdf.verbose)
    }
    options(dbdf.verbose = v)
}
dbv <-
function (df, filename = NULL, overwrite = F, index = "default", 
    tname = NULL, chunk.size = 100, read.func = read.delim, ...) 
{
    if (!is.vector(df)) 
        stop("Try dbdf instead")
    v <- dbdf(df, filename, overwrite, index, tname, chunks, 
        read.func, ...)
    attr(v, "class") <- c("dbv", "dbdf", "vector", "data.frame")
    attr(v, "cname") <- "df"
    v
}
dbv2 <-
function (s, column) 
{
    if (!column %in% colnames(s)) 
        stop("Invalid column name")
    v <- s
    attr(v, "class") <- c("dbv", "dbdf", "vector", "data.frame")
    attr(v, "cname") <- column
    v
}
dim.dbdf <-
function (s) 
c(attr(s, "nrow"), attr(s, "ncol"))
dimnames.dbdf <-
function (s) 
{
    con <- open.con(s)
    on.exit(close.con(con))
    rnames <- my.query(con, paste("select row_names from", attr(s, 
        "tname")))[, 1]
    if (is.dbv(s)) 
        return(list(rnames, NULL))
    cnames <- names(s)
    list(rnames, cnames)
}
driver <-
function (s, ...) 
UseMethod("driver")
driver.dbdf <-
function (s) 
{
    if (!is.null(attr(s, "driver"))) 
        return(attr(s, "driver"))
    dbdf.driver()
}
get.colname.mapping <-
function (s, con) 
{
    tmp <- my.query(con, paste("select * from ", attr(s, "tname"), 
        "_colname_map", sep = ""))
    mapping <- tmp[, 2]
    names(mapping) <- tmp[, 1]
    mapping
}
get.dbdf <-
function (fname, tname = NULL, driver = dbdf.driver()) 
{
    if (missing(fname) || is.null(fname)) {
        if (driver == "SQLite") {
            fname <- dirname(sprintf(dbdf.database(driver = driver), 
                "crap"))
            fname <- list.files(path = fname, pattern = ".db", 
                full = T)
        }
        else {
            fname <- dbdf.database(driver = driver)
        }
    }
    else if (driver == "SQLite" && !file.exists(fname)) {
        fname <- sprintf(dbdf.database(driver = driver), DBI:::make.db.names.default(fname))
        if (!file.exists(fname)) 
            stop(paste(fname, "doesn't exist"))
    }
    out <- list()
    for (f in fname) {
        if (is.null(tname)) {
            con <- open.con(f, driver)
            tn <- dbListTables(con)
            close.con(con)
            tn <- tn[-grep("_colname_map$", tn, perl = T)]
        }
        else {
            tn <- tname
        }
        out[[f]] <- lapply(tn, function(i) {
            out <- list()
            attr(out, "class") <- c("dbdf", "data.frame")
            attr(out, "filename") <- f
            attr(out, "tname") <- i
            attr(out, "nrow") <- length(rownames(out))
            attr(out, "ncol") <- length(colnames(out))
            attr(out, "driver") <- driver
            if (driver == "MySQL") 
                attr(out, "dbname") <- dbGetInfo(con)$dbname
            out
        })
        names(out[[f]]) <- tn
    }
    if (length(out) == 1) 
        out <- out[[1]]
    else if (length(out) < 1) 
        out <- NULL
    out
}
getMetaInfo <-
function (s, ...) 
{
    tname <- paste(attr(s, "tname"), "meta_info", sep = "_")
    con <- open.con(s)
    on.exit(close.con(con))
    if (!dbExistsTable(con, tname)) 
        return(list())
    if (dbdf.verbose()) 
        cat("Reading meta info from", dbdf.driver(), "table", 
            tname, "\n")
    out <- dbReadTable(con, tname)
    if (length(list(...)) > 0) 
        out <- out[unlist(list(...)), ]
    tmp <- as.list(as.character(out[, 2]))
    names(tmp) <- as.character(out[, 1])
    tmp
}
head.dbdf <-
function (s, n = 6, ...) 
s[1:n, ]
is.dbdf <-
function (s) 
"dbdf" %in% class(s)
is.dbv <-
function (v) 
"dbv" %in% class(v)
length.dbdf <-
function (s) 
ncol(s)
length.dbv <-
function (v) 
nrow(as.dbdf(v))
ls.dbdf <-
function () 
names(which(sapply(sapply(ls(env = parent.frame()), get), is.dbdf)))
merge.dbdf <-
function (x, y, by = intersect(names(x), names(y)), by.x = by, 
    by.y = by, all = FALSE, all.x = all, all.y = all, sort = TRUE, 
    suffixes = c(".x", ".y"), incomparables = NULL, out.dbdf = F, 
    ...) 
{
    f1 <- attr(x, "filename")
    f2 <- attr(y, "filename")
    if (f1 != f2) 
        stop(paste("Different database files currently not allowed:", 
            f1, f2))
    t1 <- attr(x, "tname")
    t2 <- attr(y, "tname")
    if (!out.dbdf) 
        query <- paste("select * from ", t1, ",", t2, sep = "")
    if (length(by.x) > 0) {
        query <- paste(query, " where ", t1, ".", by.x, "=", 
            t2, ".", by.y, sep = "")
        if (sort) 
            query <- paste(query, " order by ", t1, ".", by.x, 
                ",", t2, ".", by.y, sep = "")
    }
    if (out.dbdf) 
        query <- paste("create table merged_", t1, "_", t2, " as ", 
            query, sep = "")
    con <- open.con(x)
    on.exit(close.con(con))
    out <- my.query(con, query)
    if (!out.dbdf) {
        rownames(out) <- make.unique(out$row_names)
        out <- out[, !colnames(out) %in% c("row_idx", "row_names")]
        out <- out[, colnames(out) != make.unique(c(by.x, by.y))[2]]
        return(out)
    }
    else {
    }
}
my.query <-
function (con, qq, ...) 
{
    if (dbdf.verbose()) 
        cat("DBDF:", qq, "\n")
    dbGetQuery(con, qq, ...)
}
`names<-.dbdf` <-
function (s, value) 
stop("Not supported for dbdf's")
names.dbdf <-
function (s) 
{
    con <- open.con(s)
    on.exit(close.con(con))
    my.query(con, paste("select orig from ", attr(s, "tname"), 
        "_colname_map", sep = ""))[, 1]
}
`names<-.dbv` <-
function (v, value) 
{
    rownames(v) <- value
    v
}
names.dbv <-
function (v) 
dimnames(v)[[1]]
open.con <-
function (filename = ":memory:", drv = dbdf.driver()) 
{
    if (drv == "SQLite") 
        require(RSQLite)
    else if (drv == "MySQL") 
        require(RMySQL)
    dbname <- filename
    if (is.dbdf(filename)) {
        s <- filename
        if (!is.null(attr(s, "driver"))) 
            drv <- attr(s, "driver")
        filename <- attr(s, "filename")
        if (!is.null(attr(s, "dbname"))) 
            dbname <- attr(s, "dbname")
        else dbname <- filename
    }
    m <- DBI:::dbDriver(drv)
    cons <- dbListConnections(m)
    if (drv == "SQLite") {
        if (filename == "mem" || is.null(filename)) 
            filename <- dbname <- ":memory:"
        f.exists <- filename == ":memory:" || file.exists(filename) || 
            drv != "SQLite"
        if (!f.exists && !file.exists(dirname(filename))) 
            dir.create(dirname(filename), recursive = T)
    }
    else if (drv == "MySQL") {
        tmp.con <- dbConnect(m, group = filename)
        dbname <- dbGetInfo(tmp.con)$dbname
        dbDisconnect(tmp.con)
    }
    tabs <- lapply(cons, function(i) dbGetInfo(i)$dbname)
    exists.already <- dbname %in% unlist(tabs)
    if (exists.already) {
        wh <- which(sapply(tabs, function(i) dbname %in% i))
        con <- cons[[wh[1]]]
        attr(con, "not.new") <- TRUE
    }
    else {
        if (dbdf.verbose()) 
            cat("DBDF: Opening", drv, "connection to:", filename, 
                "\n")
        if (drv == "SQLite") 
            con <- dbConnect(m, dbname = filename)
        else if (drv == "MySQL") 
            con <- dbConnect(m, group = filename)
        attr(con, "not.new") <- filename == ":memory:"
    }
    con
}
print.dbdf <-
function (s, n = nrow(s), ...) 
{
    cat("DBDF stored in table ", attr(s, "tname"), " in ", driver(s), 
        " DB ", attr(s, "filename"), ":\n", sep = "")
    if (!is.dbv(s) && n != nrow(s)) 
        print(head(s, n = n, ...))
    else if (is.dbv(s) && n != length(s)) 
        print(head(s, n = n, ...))
    else print(s[])
}
rbind.dbdf <-
function (s, df, filename = attr(s, "filename"), tname = attr(s, 
    "tname"), chunk.size = 100) 
{
    con <- open.con(filename, driver(s))
    on.exit(close.con(con))
    new.s <- dbdf(s, filename, tname, chunk.size)
    if (!is.dbdf(df)) 
        chunk.size <- nrow(df)
    ch.inds <- c(seq(1, nrow(df), by = chunk.size), nrow(df) + 
        1)
    for (i in 1:(length(ch.inds) - 1)) {
        tmp <- df[ch.inds[i]:(ch.inds[i + 1] - 1), ]
        tmp <- cbind(row_idx = ch.inds[i]:(ch.inds[i + 1] - 1) + 
            nrow(s), row_names = rownames(tmp), tmp)
        if (dbdf.verbose()) 
            cat("DBDF: Appending", nrow(tmp), "rows to table", 
                tname, "in file", filename, "\n")
        dbWriteTable(con, attr(new.s, "tname"), tmp, row.names = F, 
            append = T)
    }
    attr(new.s, "nrow") <- attr(s, "nrow") + nrow(df)
    if (driver(s) == "MySQL") 
        attr(new.s, "dbname") <- dbGetInfo(con)$dbname
    new.s
}
reshape.dbdf.out <-
function (out, s) 
{
    if (is.null(out) || sum(dim(out)) == 0) 
        return(out)
    rnames <- make.unique(out$row_names)
    if (is.dbv(s)) {
        out <- out[, 3]
        names(out) <- rnames
        return(out)
    }
    rownames(out) <- make.unique(rnames)
    out <- out[, -(1:2), drop = F]
    names(out) <- names(s)
    out
}
rm.dbdf <-
function (s) 
{
    if (attr(s, "rw") == "r") 
        stop("dbdf is read.only (see allow.write())")
    con <- open.con(s)
    on.exit(close.con(con))
    my.query(con, paste("drop table", attr(s, "tname")))
    my.query(con, paste("drop table ", attr(s, "tname"), "_colname_map", 
        sep = ""))
    if (dbExistsTable(con, paste(attr(s, "tname"), "_meta_info", 
        sep = ""))) 
        my.query(con, paste("drop table ", attr(s, "tname"), 
            "_meta_info", sep = ""))
    if (driver(s) == "SQLite" && length(dbListTables(con)) <= 
        0 && attr(s, "filename") != ":memory:") 
        try(file.remove(attr(s, "filename")))
    rm(list = deparse(substitute(s)), envir = sys.frame(sys.parent()))
}
`row.names<-.dbdf` <-
function (s, value) 
{
    s[, "row_names"] <- value
    s
}
select <-
function (s, ...) 
UseMethod("select")
select.dbdf <-
function (s, select = "*", where = "", limit = "", order = "", 
    group.by = "") 
{
    require(gsubfn)
    if (is.dbv(s)) 
        select <- paste("row_idx", "row_names", attr(s, "cname"), 
            sep = ",")
    else {
        select <- gsubfn(, , select, env = sys.frame(sys.parent()))
        select <- gsub("c\\(", "", select, perl = T)
        select <- gsub("\\)", "", select, perl = T)
    }
    con <- open.con(s)
    on.exit(close.con(con))
    mapping <- get.colname.mapping(s, con)
    for (m in names(mapping)) {
        select <- gsub(m, mapping[m], select, fixed = T)
        where <- gsub(m, mapping[m], where, fixed = T)
        limit <- gsub(m, mapping[m], limit, fixed = T)
        order <- gsub(m, mapping[m], order, fixed = T)
        group.by <- gsub(m, mapping[m], group.by, fixed = T)
    }
    where <- if (where != "") 
        paste("where", gsubfn(, , where, env = sys.frame(sys.parent())))
    else ""
    limit <- if (limit != "") 
        paste("limit", gsubfn(, , as.character(limit), env = sys.frame(sys.parent())))
    else ""
    order <- if (order != "") 
        paste("order by", gsubfn(, , as.character(order), env = sys.frame(sys.parent())))
    else ""
    group.by <- if (group.by != "") 
        paste("group by", gsubfn(, , as.character(group.by), 
            env = sys.frame(sys.parent())))
    else ""
    if (driver(s) == "SQLite") 
        cmd <- paste("select ", select, " from [", attr(s, "tname"), 
            "] ", where, " ", limit, " ", order, " ", group.by, 
            sep = "")
    else cmd <- paste("select ", select, " from ", attr(s, "tname"), 
        " ", where, " ", limit, " ", order, " ", group.by, sep = "")
    cmd <- gsub("c(", "(", cmd, fixed = T)
    out <- my.query(con, cmd)
    if (is.null(out) || sum(dim(out)) == 0) 
        return(out)
    if ("row_idx" %in% colnames(out) && !"row_idx" %in% select) {
        out <- out[, -which(colnames(out) == "row_idx")]
    }
    if ("row_names" %in% colnames(out) && !"row_names" %in% select) {
        rownames(out) <- make.unique(out$row_names)
        out <- out[, -which(colnames(out) == "row_names")]
    }
    for (m in mapping) colnames(out)[colnames(out) == m] <- names(mapping)[mapping == 
        m]
    out
}
setMetaInfo <-
function (s, ...) 
UseMethod("setMetaInfo")
setMetaInfo.dbdf <-
function (s, ...) 
{
    con <- open.con(s)
    on.exit(close.con(con))
    tname <- paste(attr(s, "tname"), "meta_info", sep = "_")
    tmp <- data.frame(name = names(unlist(list(...))), meta_value = as.character(sapply(unlist(list(...)), 
        function(i) i)))
    tmp <- cbind(row_names = tmp[, 1], tmp)
    if (dbExistsTable(con, tname)) {
        if (dbdf.verbose()) 
            cat("Reading", dbdf.driver(), "table", tname, "\n")
        meta <- dbReadTable(con, tname)
        meta <- cbind(row_names = rownames(meta), meta)
        meta <- merge(tmp, meta, all = T)
    }
    else {
        meta <- tmp
    }
    if (dbdf.verbose()) 
        cat("DBDF: writing meta info to", dbdf.driver(), "table", 
            tname, "\n")
    dbWriteTable(con, tname, meta, row.names = F, append = F, 
        overwrite = T)
    meta
}
str.dbdf <-
function (s) 
str(s[1:11, ])
subset.dbdf <-
function (s, subset, select, drop = F, ...) 
{
    require(gsubfn)
    e <- ""
    cnm <- colnames(s)
    tmp <- 1:length(cnm)
    names(tmp) <- cnm
    tmp <- as.list(tmp)
    attach(tmp)
    on.exit(detach(tmp))
    if (!missing(subset)) {
        e <- paste(deparse(substitute(subset)), collapse = " ")
        e <- gsub(", ", ",", gsub("\\|", "OR", gsub("\\|\\|", 
            "OR", gsub("\\&", "AND", gsub("\\&\\&", "AND", gsub("%in%", 
                " in ", gsub("==", "=", gsub("\\\"", "'", e))))))))
        e <- gsub("^[\"'](.*)[\"']$", "\\1", e, perl = T)
        for (i in cnm) e <- gsub(paste("`", i, "`", sep = ""), 
            i, e)
        if (driver(s) == "SQLite") 
            for (i in cnm) e <- gsub(i, paste("[", i, "]", sep = ""), 
                e)
        e <- gsubfn(, , e, env = sys.frame(sys.parent()))
    }
    e2 <- "*"
    if (!missing(select)) {
        e2 <- deparse(substitute(select))
        e2 <- gsub(", ", ",", gsub("\\)$", "", gsub("c\\(", "", 
            e2), perl = T))
        for (i in cnm) e <- gsub(paste("`", i, "`", sep = ""), 
            i, e)
        if (driver(s) == "SQLite") 
            for (i in cnm) e2 <- gsub(i, paste("[", i, "]", sep = ""), 
                e2)
        e2 <- gsubfn(, , e2, env = sys.frame(sys.parent()))
    }
    out <- select.dbdf(s, select = e2, where = e)
    if (drop && ncol(out) == 1) 
        out <- out[, 1]
    out
}
tail.dbdf <-
function (s, n = 6, ...) 
s[(nrow(s) - n + 1):nrow(s), ]
write.default.meta.info <-
function (s, type = c("create", "access", "update")[1], ...) 
{
    meta <- c(date(), Sys.info()["user"])
    names(meta) <- paste(type, c("date", "user"), sep = ".")
    setMetaInfo(s, meta, ...)
}
