using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace NugetLibraryCollector
{
	class Program
	{
		static int Main(string[] args)
		{
			var curdir = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
			var paths = curdir.Split('\\').Reverse().ToList();

			string framework = null;
			string package = null;
			string basedir = null;
			string package_dir = null;
			try
			{
				// FIXME: Use more accurate rule
				if (string.Equals("lib", paths[0]))
				{
					package = paths[1];
					package_dir = Path.GetDirectoryName(curdir);
				}
				else
				{
					if (false == string.Equals("lib", paths[1]))
					{
						throw new IOException("The directory that executable file is located doesn't look like Nuget package.");
					}
					framework = paths[0];
					package = paths[2];
					package_dir = Path.GetDirectoryName(Path.GetDirectoryName(curdir));
				}
				basedir = Path.GetDirectoryName(package_dir);

				Console.Out.WriteLine(string.Format("Collecting libraries under '{0}' for package '{1}' and '{2}' framework", basedir, package, framework ?? "neutral"));
			}
			catch (Exception ex)
			{
				Console.Error.WriteLine(string.Format("The directory that executable file is located is invalid to process: {0}, {1}", curdir, ex.Message));
				return 1;
			}

			foreach(var d in Directory.GetDirectories(basedir)) {
				if (d.Contains(package))  // Don't work on current package
				{
					continue;				
				}

				if (Directory.Exists(Path.Combine(d, "lib")))
				{
					string source_path = null;

					// FIXME: Use more accurate rule
					if (false == string.IsNullOrWhiteSpace(framework) && Directory.Exists(Path.Combine(d, "lib", framework)))
					{
						source_path = Path.Combine(d, "lib", framework);
					}
					else
					{
						var subdirs = Directory.GetDirectories(Path.Combine(d, "lib")).ToArray();
						if (subdirs.Length == 1)  // If there is only one platform, use that one.
						{
							source_path = subdirs.First();
						}
						else if (string.IsNullOrWhiteSpace(framework))
						{
							source_path = subdirs.First();  // Just pick a random one
						}
						else
						{
							foreach (var sd in subdirs)
							{
								if (sd.Contains(framework))  // If an framework has similar name with this, use that one. Think about counter case later.
								{
									source_path = sd;
									break;
								}
							}
						}
					}

					if (String.IsNullOrWhiteSpace(source_path))
					{
						Console.Out.WriteLine(string.Format("Package '{0}' doesn't have proper version of framework. Skip to continue.", d));
					}
					else
					{
						try
						{
							CopyFiles(source_path, curdir, "*.dll");
							CopyFiles(source_path, curdir, "*.xml");
						}
						catch (IOException ex)
						{
							Console.Error.WriteLine(string.Format("Error during copy library from '{0}': {1}", d, ex.Message));
						}
					}
				}
				else 
				{
					Console.Out.WriteLine(string.Format("Package '{0}' doesn't have library. Skip to continue.", d));
				}			
			}

			return 0;
		}

		static int CopyFiles(string source_dir, string target_dir, string search_pattern) { 
			if(false == Directory.Exists(source_dir))
			{
				throw new IOException("Given path does not exists: " + source_dir);
			}
			if(false == Directory.Exists(target_dir))
			{
				throw new IOException("Given path does not exists: " + target_dir);
			}
			int copied = 0;

			foreach (var f in Directory.GetFiles(source_dir, search_pattern))
			{
				Console.Out.WriteLine(string.Format("Copying file '{0}' to '{1}'", f, Path.Combine(target_dir, Path.GetFileName(f))));
				File.Copy(f, Path.Combine(target_dir, Path.GetFileName(f)), true);
				++copied;
			}

			return copied;
		}
	}
}
