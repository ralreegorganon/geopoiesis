using System;
using System.IO;
using System.Linq;

namespace geopoiesis
{
    public class VipsPrepTask
    {
        public void Execute()
        {
            var files = Directory.GetFiles(@"H:\cddmap\alltheimages2", "oo.*");
            var blank = @"H:\cddmap\blank.png";
            var target = @"H:\cddaraster";

            // oo.0.27_10
            var wut = files.Select(x => {
                var f = Path.GetFileNameWithoutExtension(x);
                f = f.Replace("oo.", "");
                f = f.Replace("_10", "");
                var parts = f.Split(".", StringSplitOptions.RemoveEmptyEntries);
                return (f: x, x: int.Parse(parts[0]), y: int.Parse(parts[1]));
            }).ToList();

            var maxX = wut.Select(x => x.x)
                .Max();
            var maxY = wut.Select(x => x.y)
                .Max();

            Console.WriteLine($"(0,0) to ({maxX},{maxY}");

            //maxX = 10;
            //maxY = 4;

            var indexable = wut.ToDictionary(k => (k.x, k.y), v => v.f);

            for (var x = 0; x <= maxX; x++)
            {
                for (var y = 0; y <= maxY; y++)
                {
                    var output = Path.Combine(target, $"{y:D3}.{x:D3}.png");
                    var exists = indexable.TryGetValue((x, y), out var om);
                    File.Copy(exists ? om : blank, output, true);
                }
            }
        }
    }
}
