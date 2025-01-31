# retoc

CLI tool for packing/unpacking Unreal Engine IoStore containers (.utoc/.ucas) as
well as converting between Zen assets and Legacy assets (found in .pak containers).

## cli
```console
$ retoc --help
Usage: retoc [OPTIONS] <COMMAND>

Commands:
  manifest    Extract manifest from .utoc
  info        Show container info
  list        List fils in .utoc (directory index)
  unpack      Extracts chunks (files) from .utoc
  to-legacy   Converts asests and shaders from Zen to Legacy
  to-zen      Converts assets and shaders from Legacy to Zen
  help        Print this message or the help of the given subcommand(s)

Options:
  -a, --aes-key <AES_KEY>
  -h, --help               Print help
```

### to-legacy
```console
$ ls AbioticFactor/Content/Paks
global.ucas
global.utoc
pakchunk0-Windows.pak
pakchunk0-Windows.ucas
pakchunk0-Windows.utoc

$ retoc to-legacy AbioticFactor/Content/Paks legacy_P.pak
Detected package version: FPackageFileVersion(UE4: 522, UE5: 1012), EZenPackageVersion: 3
[00:00:06] ########################################   22522/22522
Extracted 22522 (0 failed) legacy assets to "legacy_P.pak"
Shader Library ShaderArchive-Global-PCD3D_SM5-PCD3D_SM5 statistics: Shared Shaders: 10; Unique Shaders: 4324; Detached Shaders: 0; Shader Maps: 339 (referenced by 0 packages), Uncompressed Size: 68MB, Compressed Size: 15MB, Compression Ratio: 445%
Shader Library ShaderArchive-AbioticFactor_Chunk0-PCD3D_SM5-PCD3D_SM5 statistics: Shared Shaders: 1941; Unique Shaders: 12327; Detached Shaders: 0; Shader Maps: 819 (referenced by 1721 packages), Uncompressed Size: 138MB, Compressed Size: 43MB, Compression Ratio: 322%
Extracted 2 shader code libraries to "legacy_P.pak"
```

### to-zen
```console
$ retoc to-zen legacy_P.pak iostore.utoc --version UE5_4
converting shader library "AbioticFactor/Content/ShaderArchive-AbioticFactor_Chunk0-PCD3D_SM5-PCD3D_SM5.ushaderbytecode"
Shader Library ShaderArchive-AbioticFactor_Chunk0-PCD3D_SM5-PCD3D_SM5 statistics: Shader Groups: 1115, Shader Maps: 819, Uncompressed Size: 138MB, Original Compressed Size: 43MB, Total Group Compressed Size: 10MB, Recompression Ratio: 397%, Total Compression Ratio: 1277%
converting shader library "AbioticFactor/Content/ShaderArchive-Global-PCD3D_SM5-PCD3D_SM5.ushaderbytecode"
Shader Library ShaderArchive-Global-PCD3D_SM5-PCD3D_SM5 statistics: Shader Groups: 392, Shader Maps: 339, Uncompressed Size: 68MB, Original Compressed Size: 15MB, Total Group Compressed Size: 6MB, Recompression Ratio: 222%, Total Compression Ratio: 987%
[00:00:02] ########################################   22522/22522

$ ls
legacy_P.pak
iostore.ucas
iostore.utoc
iostore.pak
...
```

## compatibility
Unreal Engine versions 5.3+ are well supported and can convert entire games to
.pak and have them run without issue. Lack of dependency information in versions
prior to 5.3 may result in games failing to load a handful of assets, preventing
them from running from .pak, but should not be an issue for modding purposes.

## credits
- [Archengius](https://github.com/Archengius): writing all of the asset conversion code
- [LongerWarrior](https://github.com/LongerWarrior): debugging complex conversion issues and testing against many games
