﻿namespace geopoiesis
{
    class Program
    {
        static void Main(string[] args)
        {
            var sqlConnectionString = "Server=localhost;Database=Cataclysm2;Trusted_connection=true";
            var sqlitePath = @"H:\cddmap\cataclysm2.sqlite3";
            var overmapSubsetIds = new int [] { };
            var overmapOutput = @"F:\code\cpp\Cataclysm-DDA\save\refinebuilding\o.{0}.{1}";

            //var createViewsTask = new CreateViewsTask(sqlConnectionString, overmapSubsetIds);
            //createViewsTask.Execute();

            //var writeSqliteTask = new WriteSqliteTask(sqlConnectionString, sqlitePath, overmapSubsetIds);
            //writeSqliteTask.Execute();

            var writeOvermapsTask = new WriteOvermapsTask(sqlitePath, overmapOutput);
            writeOvermapsTask.Execute();
        }
    }
}
