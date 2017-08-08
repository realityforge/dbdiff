require 'buildr/git_auto_version'

desc 'dbdiff: List differences between databases'
define 'dbdiff' do
  project.group = 'org.realityforge.dbdiff'
  compile.options.source = '1.8'
  compile.options.target = '1.8'
  compile.options.lint = 'all'

  compile.with :getopt4j,
               :diffutils,
               :jtds,
               :postgresql

  test.using :testng

  package(:jar)
  package(:jar, :classifier => 'all').tap do |jar|
    jar.merge(artifact(:getopt4j))
    jar.merge(artifact(:diffutils))
  end
end
