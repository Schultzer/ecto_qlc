Enum.map(File.ls!("priv/repo/migrations"), &Code.compile_file(Path.join("priv/repo/migrations", &1)))
ExUnit.start()
