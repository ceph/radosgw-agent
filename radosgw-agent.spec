%define name radosgw-agent
%define version 1.1
%define unmangled_version 1.1
%define unmangled_version 1.1
%define release 1

Summary: Synchronize users and data between radosgw clusters
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: MIT
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Josh Durgin <josh.durgin@inktank.com>
Requires: python-argparse
Requires: PyYAML
Requires: python-boto >= 2.2.2
Requires: python-boto < 3.0.0
Requires: python-requests
Url: https://github.com/ceph/radosgw-agent

%description
UNKNOWN

%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
