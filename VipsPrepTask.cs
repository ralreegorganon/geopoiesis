using System;
using System.IO;
using System.Linq;

namespace geopoiesis
{
    public class VipsPrepTask
    {
        public void Execute()
        {
            var files = Directory.GetFiles(@"/Users/jj/Desktop/o", "oo.*");
            var blank = @"/Users/jj/Desktop/blank.png";
            var target = @"/Users/jj/Desktop/cddaraster";

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
            var minX = wut.Select(x => x.x)
                .Min();
            var maxY = wut.Select(x => x.y)
                .Max();
            var minY = wut.Select(x => x.y)
                .Min();

            var w = maxX - minX + 1;
            var h = maxY - minY + 1;

            Console.WriteLine($"WxH: {w}x{h} ({minX},{minY}) to ({maxX},{maxY})");

            //maxX = 10;
            //maxY = 4;

            var indexable = wut.ToDictionary(k => (k.x-minX, k.y-minY), v => v.f);

            for (var x = 0; x < w; x++)
            {
                for (var y = 0; y < h; y++)
                {
                    var output = Path.Combine(target, $"{y:D3}.{x:D3}.png");
                    var exists = indexable.TryGetValue((x, y), out var om);
                    File.Copy(exists ? om : blank, output, true);
                }
            }
        }
    }
}
