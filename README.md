DbDiff - List differences between databases
===========================================

[![Build Status](https://secure.travis-ci.org/realityforge/dbdiff.svg?branch=master)](http://travis-ci.org/realityforge/dbdiff)
[<img src="https://img.shields.io/maven-central/v/org.realityforge.dbdiff/dbdiff.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.realityforge.dbdiff%22%20a%3A%22dbdiff%22)

What is DbDiff
--------------

DbDiff is a simple command line application that identifies schema differences
between databases. Unlike other tools in this space, it does not attempt to help
you resolve differences.

TODO
----

Currently the tool uses the built in jdbc mechanisms for getting at databases
metadata. This leaves us at the mercy of the underlying jdbc driver and not
all of them seem to implement the full suite of methods. Also there is currently
several elements of the database schema that are not exposed through an API. In
most cases the underlying database provides access to the features through custom
SQL.

So in the future we should use vendor specific SQL to get access to database features
that we need to check. These currently include but are not limited to;

* Check constraints
* Functions in SQL Server
* Rules in Postgres
* View, Stored Procedure, Trigger, Rule and Function definitions/code.

We also need to ensure that we correctly maintain certain fixture tables. So we should
make it possible to do diffs against particular tables or even against particular
views/queries across databases.

We also need the ability to add elements to a white list that we will ignore. i.e. the
migrations tables, spatial_ref_sys, geometry_columns etc.
