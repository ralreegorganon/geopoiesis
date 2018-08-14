using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Dapper;
using Microsoft.Data.Sqlite;
using QuickGraph;
using QuickGraph.Algorithms.MinimumSpanningTree;

namespace geopoiesis
{
    public class TransformOvermapTerrainTask
    {
        private string sqlitePath;
        private string overmapOutput;

        public TransformOvermapTerrainTask(string sqlitePath, string overmapOutput)
        {
            this.sqlitePath = sqlitePath;
            this.overmapOutput = overmapOutput;
        }

        public void Execute()
        {
            List<Omt> all = null;
            var g = new UndirectedGraph<Omt, OmtConnection>(false);

            using (var c = new SqliteConnection($"Data Source={sqlitePath}"))
            {
                DefaultTypeMap.MatchNamesWithUnderscores = true;

                all = c.Query<Omt>(@"
                    select 
                        om_pagenumber,
                        omt_pagenumber,
                        om_y,
                        om_x,
                        omt_y,
                        omt_x,
                        ifnull(land_use_code, -1) land_use_code,
                        ifnull(primary_road_type, -1) primary_road_type
                    from 
                        omt 
                    where 
                        om_x = 22 and om_y = 22
                    order by
                        om_pagenumber,
                        omt_pagenumber
                ").ToList();

                for (var i = 0; i < all.Count; i++)
                {
                    var current = all[i];

                    if (current.PrimaryRoadType == -1)
                    {
                        continue;
                    }

                    // W
                    if (i % 180 > 0)
                    {
                        var west = all[i - 1];
                        if (west.PrimaryRoadType != -1)
                        {
                            g.AddVerticesAndEdge(new OmtConnection(current, west, true));
                        }
                    }

                    // E
                    if (i % 179 != 0 || i == 0)
                    {
                        var east = all[i + 1];
                        if (east.PrimaryRoadType != -1)
                        {
                            g.AddVerticesAndEdge(new OmtConnection(current, east, true));
                        }
                    }

                    // N
                    if (i > 179)
                    {
                        var north = all[i - 180];
                        if (north.PrimaryRoadType != -1)
                        {
                            g.AddVerticesAndEdge(new OmtConnection(current, north, true));
                        }
                    }

                    // S
                    if (i < 32220)
                    {
                        var south = all[i + 180];
                        if (south.PrimaryRoadType != -1)
                        {
                            g.AddVerticesAndEdge(new OmtConnection(current, south, true));
                        }
                    }

                    // NW
                    if (i % 180 > 0 && i > 179)
                    {
                        var west = all[i - 181];
                        if (west.PrimaryRoadType != -1)
                        {
                            g.AddVerticesAndEdge(new OmtConnection(current, west, false));
                        }
                    }

                    // NE
                    if ((i % 179 != 0 || i == 0) && i > 179)
                    {
                        var east = all[i - 179];
                        if (east.PrimaryRoadType != -1)
                        {
                            g.AddVerticesAndEdge(new OmtConnection(current, east, false));
                        }
                    }

                    // SW
                    if (i < 32220 && i % 180 > 0)
                    {
                        var north = all[i + 179];
                        if (north.PrimaryRoadType != -1)
                        {
                            g.AddVerticesAndEdge(new OmtConnection(current, north, false));
                        }
                    }

                    // SE
                    if (i < 32220 && (i % 179 != 0 || i == 0))
                    {
                        var south = all[i + 181];
                        if (south.PrimaryRoadType != -1)
                        {
                            g.AddVerticesAndEdge(new OmtConnection(current, south, false));
                        }
                    }
                }
            }

            var sources = g.Vertices.Where(x => g.AdjacentDegree(x) > 4)
                .Where(x =>
                {
                    var adjacentEdges = g.AdjacentEdges(x);
                    var c = adjacentEdges.Where(z => z.IsPrimary)
                        .Any(y => !Equals(y.Target, x) && g.AdjacentDegree(y.Target) > 4);
                    return c;
                })
                .ToList();

            foreach (var s in sources)
            {
                s.PrimaryRoadType = -2;
            }

            var z10overmaps = all.GroupBy(x => (x.OmX, x.OmY)).ToDictionary(k => k.Key, v => v.ToList());

            foreach (var key in z10overmaps.Keys)
            {
                var z10om = z10overmaps[key];

                var z10Text = OvermapTerrainToText(z10om);

                var template = File.ReadAllLines("omtemplate.txt");
                template[11] = z10Text;
                template[12] = z10Text;

                File.WriteAllLines(string.Format(@"F:\code\cpp\Cataclysm-DDA\save\Florida\o.{0}.{1}", key.Item1, key.Item2), template);
            }
        }

        private string OvermapTerrainToText(List<Omt> omt)
        {
            OvermapTerrainGroup current = null;

            var list = new List<OvermapTerrainGroup>();

            foreach (var r in omt)
            {
                if (Type(r.LandUseCode.Value, r.PrimaryRoadType.Value) == current?.Type)
                {
                    current.Count++;
                }
                else
                {
                    if (current != null)
                    {
                        list.Add(current);
                    }

                    current = new OvermapTerrainGroup {
                        Type = Type(r.LandUseCode.Value, r.PrimaryRoadType.Value),
                        Count = 1
                    };
                }
            }
            list.Add(current);

            var inner = string.Join(",", list.Select(x => $"[\"{x.Type}\",{x.Count}]"));
            var outer = $",[{inner}]";

            return outer;
        }

        private string Type(int landUseCode, int roadType)
        {
            if (roadType == -3)
            {
                return "sewage_treatment";
            }

            if (roadType == -2)
            {
                return "sewage_treatment";
            }

            if (roadType > 0)
            {
                return "road_ns";
            }



            switch (landUseCode)
            {
                case -1: return "river";
                case 40: return "field";
                case 34: return "cemetery_small_north";
                case 15: return "s_gun_north";
                case 23: return "pond_swamp";
                case 1: return "farm_1";
                case 3: return "forest";
                case 37: return "forest_water";
                case 26: return "park";
                case 11: return "house_north";
                case 16: return "small_storage_units_north";
                case 39: return "toxic_dump";
                case 13: return "house_north";
                case 29: return "fishing_pond_0_0_north";
                case 12: return "house_north";
                case 5: return "mine";
                case 10: return "apartments_con_tower_NE_north";
                case 4: return "pond_swamp";
                case 36: return "orchard_tree_apple";
                case 6: return "field";
                case 35: return "orchard_tree_apple";
                case 7: return "park";
                case 2: return "farm_1_north";
                case 24: return "pwr_sub_s";
                case 25: return "dirtlot";
                case 14: return "pond_swamp";
                case 8: return "gym";
                case 17: return "field";
                case 18: return "spiral";
                case 31: return "police_north";
                case 38: return "house_north";
                case 19: return "sewage_treatment";
                case 20: return "river_north";
                case 9: return "fishing_pond_0_0_north";
                default: return "forest";
            }
        }
    }

    public class OmtConnection : Edge<Omt>
    {
        public bool IsPrimary { get; set; }

        public OmtConnection(Omt source, Omt target, bool isPrimary) : base(source, target)
        {
            IsPrimary = isPrimary;
        }
    }

    public class Omt
    {
        public int OmPagenumber { get; set; }
        public int OmtPagenumber { get; set; }
        public int OmY { get; set; }
        public int OmX { get; set; }
        public int OmtY { get; set; }
        public int OmtX { get; set; }
        public int? LandUseCode { get; set; }
        public int? PrimaryRoadType { get; set; }
        public string AllRoadTypes { get; set; }
        public string SubwayPrimaryLine { get; set; }
        public string SubwayAllLines { get; set; }
        public string RapidTransitPrimaryLine { get; set; }
        public int? RapidTransitPrimaryLineGrade { get; set; }
        public string RapidTransitAllLines { get; set; }
        public string RapidTransitAllLinesGrades { get; set; }
        public string TransitStationLine { get; set; }
        public bool? TransitStationIsTerminus { get; set; }
        public string MilitaryBaseComponent { get; set; }
        public string MilitaryBaseName { get; set; }
        public int? TownId { get; set; }
        public int? TrailClass { get; set; }
        public int? TrainType { get; set; }
        public bool? Station { get; set; }
        public bool? Ocean { get; set; }
        public string PrimaryStructureId { get; set; }
        public double? PrimaryStructureTotalAreaSqM { get; set; }
        public int? PrimaryStructureOtherCellCoverCount { get; set; }
        public int? CellStructureCount { get; set; }
        public double? CellStructureTotalPercentageCovered { get; set; }

        public override bool Equals(object obj)
        {
            return obj is Omt omt &&
                   OmPagenumber == omt.OmPagenumber &&
                   OmtPagenumber == omt.OmtPagenumber;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(OmPagenumber, OmtPagenumber);
        }
    }
}
