Special instructions for compiling/running the code should be included in this file.

Tony - Use following commands for convenience: 
go run client.go 127.0.0.1:3001 127.0.0.1:2001
go run frontend.go 127.0.0.1:2001 127.0.0.1:2002 127.0.0.1:2003 127.0.0.1:2004 127.0.0.1:2005 127.0.0.1:2006 2
go run metadata.go 127.0.0.1:2011 127.0.0.1:2002 127.0.0.1:2012 127.0.0.1:2013 127.0.0.1:2014 2
go run auth.go 127.0.0.1:2012 127.0.0.1:2003 
go run filestoreA.go 127.0.0.1:2013 127.0.0.1:2004 127.0.0.1:2012 2
go run filestoreB.go 127.0.0.1:2014 127.0.0.1:2005 2

Replicas:
go run metadata.go 127.0.0.1:2015 127.0.0.1:2002 127.0.0.1:2012 127.0.0.1:2013 127.0.0.1:2014 2
go run metadata.go 127.0.0.1:2016 127.0.0.1:2002 127.0.0.1:2012 127.0.0.1:2013 127.0.0.1:2014 2
go run filestoreA.go 127.0.0.1:2017 127.0.0.1:2004 127.0.0.1:2012 2
go run filestoreA.go 127.0.0.1:2018 127.0.0.1:2004 127.0.0.1:2012 2
go run filestoreB.go 127.0.0.1:2019 127.0.0.1:2005 2
go run filestoreB.go 127.0.0.1:2020 127.0.0.1:2005 2


The End.
