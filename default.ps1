properties { 
  $base_dir  = resolve-path .
  $lib_dir = "$base_dir\SharedLibs"
  $build_dir = "$base_dir\build" 
  $buildartifacts_dir = "$build_dir\" 
  $sln_file = "$base_dir\Rhino.Queues.sln" 
  $version = "1.2.0.0"
  $tools_dir = "$base_dir\Tools"
  $release_dir = "$base_dir\Release"
} 

include .\psake_ext.ps1
	
task default -depends Release

task Clean { 
  remove-item -force -recurse $buildartifacts_dir -ErrorAction SilentlyContinue 
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
        
    Generate-Assembly-Info `
		-file "$base_dir\Rhino.Queues.Visualizer\Properties\AssemblyInfo.cs" `
		-title "Rhino Queues $version" `
		-description "HTTP based reliable async queuing system" `
		-company "Hibernating Rhinos" `
		-product "Rhino Queues $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2009" `
        -clsCompliant "false"
		
	new-item $release_dir -itemType directory 
	new-item $buildartifacts_dir -itemType directory 
} 

task Compile -depends Init { 
  exec msbuild "/p:OutDir=""$buildartifacts_dir "" $sln_file"
} 

task Test -depends Compile {
  $old = pwd
  cd $build_dir
  exec "$tools_dir\xUnit\xunit.console.exe" "$build_dir\Rhino.Queues.Tests.dll"
  cd $old		
}


task Release -depends Test {
	& $tools_dir\zip.exe -9 -A -j `
		$release_dir\Rhino.Queues.zip `
        $build_dir\Rhino.Queues.dll `
        $build_dir\Rhino.Queues.xml `
        $build_dir\Esent.Interop.dll `
		$build_dir\Esent.Interop.xml `
		license.txt `
		acknowledgements.txt
	if ($lastExitCode -ne 0) {
        throw "Error: Failed to execute ZIP command"
    }
}