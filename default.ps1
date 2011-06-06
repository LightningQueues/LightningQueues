properties {
  $base_dir  = resolve-path .
  $lib_dir = "$base_dir\SharedLibs"
  $build_dir = "$base_dir\build" 
  $40_build_dir = "$build_dir\4.0\"
  $35_build_dir = "$build_dir\3.5\"
  $sln_file = "$base_dir\Rhino.Queues.sln" 
  $version = "1.2.1.0"
  $config = "Release"
  $tools_dir = "$base_dir\Tools"
  $release_dir = "$base_dir\Release"
}

$framework = "4.0"

include .\psake_ext.ps1

task default -depends Release

task Clean {
  remove-item -force -recurse $build_dir -ErrorAction SilentlyContinue 
  remove-item -force -recurse $release_dir -ErrorAction SilentlyContinue 
}

task Init -depends Clean {
	Generate-Assembly-Info `
		-file "$base_dir\Rhino.Queues\Properties\AssemblyInfo.cs" `
		-title "Rhino Queues $version" `
		-description "HTTP based reliable async queuing system" `
		-company "Hibernating Rhinos" `
		-product "Rhino Queues $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2009" `
        -clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Rhino.Queues.Tests\Properties\AssemblyInfo.cs" `
		-title "Rhino Queues $version" `
		-description "HTTP based reliable async queuing system" `
		-company "Hibernating Rhinos" `
		-product "Rhino Queues $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2009" `
        -clsCompliant "false"

	new-item $release_dir -itemType directory 
	new-item $build_dir -itemType directory 
}

task Compile40 -depends Init {
  msbuild $sln_file /p:"OutDir=$40_build_dir;Configuration=$config;TargetFrameworkVersion=V4.0"
}

task Compile35 -depends Init {
  msbuild $sln_file /p:"OutDir=$35_build_dir;Configuration=$config;TargetFrameworkVersion=V3.5"
}

task Test -depends Compile35, Compile40 {
  $old = pwd
  cd $build_dir
  & $tools_dir\xUnit\xunit.console.exe "$35_build_dir\Rhino.Queues.Tests.dll"
  cd $old
}


task Release -depends Test {
  cd $build_dir
  & $tools_dir\7za.exe a $release_dir\Rhino.Queues.zip `
        *\Rhino.Queues.dll `
        *\Rhino.Queues.pdb `
        *\log4net.dll `
        *\Rhino.Queues.xml `
        *\Esent.Interop.dll `
        *\Esent.Interop.xml `
        *\Wintellect.Threading.dll `
        *\Wintellect.Threading.xml `
        license.txt `
        acknowledgements.txt
    if ($lastExitCode -ne 0) {
        throw "Error: Failed to execute ZIP command"
    }
}
