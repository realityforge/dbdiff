require 'buildr/git_auto_version'

desc 'dbdiff: List differences between databases'
define 'dbdiff' do
  project.group = 'org.realityforge.dbdiff'
  compile.options.source = '1.6'
  compile.options.target = '1.6'
  compile.options.lint = 'all'

  compile.with :spice_cli,
               :diffutils,
               :postgresql

  package(:jar)
  package(:jar, :classifier => 'all').tap do |jar|
    jar.merge(artifact(:spice_cli))
    jar.merge(artifact(:diffutils))
  end
end
