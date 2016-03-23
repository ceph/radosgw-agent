Summary: Synchronize users and data between radosgw clusters
Name: radosgw-agent
Version:       1.2.4
Release: 0%{?dist}
Source0: https://pypi.python.org/packages/source/r/%{name}/%{name}-%{version}.tar.gz
License: MIT
Group: Development/Libraries
BuildArch: noarch
Requires: python-argparse
Requires: PyYAML
Requires: python-boto >= 2.10.0
Requires: python-boto < 3.0.0
BuildRequires: python-devel
BuildRequires: python-setuptools
URL: https://github.com/ceph/radosgw-agent

%description
The Ceph RADOS Gateway agent replicates the data of a master zone to a
secondary zone.

%prep
%setup -q

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT
install -m 0755 -D scripts/radosgw-agent $RPM_BUILD_ROOT%{_bindir}/radosgw-agent
install -m 0644 -D logrotate.conf $RPM_BUILD_ROOT%{_sysconfdir}/logrotate.d/radosgw-agent
install -m 0755 -D init-radosgw-agent $RPM_BUILD_ROOT%{_initrddir}/radosgw-agent
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/ceph/radosgw-agent
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/log/ceph/radosgw-agent
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/run/ceph/radosgw-agent

%files
%doc LICENSE
%dir %{_sysconfdir}/ceph/radosgw-agent
%dir %{_localstatedir}/log/ceph/radosgw-agent
%dir %{_localstatedir}/run/ceph/radosgw-agent
%config(noreplace) %{_sysconfdir}/logrotate.d/radosgw-agent
%{_bindir}/radosgw-agent
%{_initrddir}/radosgw-agent
%{python_sitelib}/radosgw_agent*/
