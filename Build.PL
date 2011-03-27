use strict;
use warnings;

use Module::Build;

my $build = Module::Build->new(
	module_name => 'WWW::GoodData',
	license => 'perl',
	dist_author => 'Lubomir Rintel <lkundrak@v3.sk>',
	dist_version => '0.1',
	meta_merge => {
		resources => {
			bugtracker => 'http://rt.cpan.org/NoAuth/Bugs.html?Dist=WWW-GoodData',
			repository => 'https://github.com/lkundrak/www-gooddata',
		}
	},
	requires => {
		'Getopt::Long' => 0,
		'Pod::Usage' => 0,
		'Text::ParseWords' => 0,
	},
	build_requires => {}
);

$build->create_build_script;
