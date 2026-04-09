{
  DataEngine — backward-compatibility shim.
  Re-exports DataEngine.FactoryInterfaces under the legacy dotted-lowercase name
  so consumers that still reference 'DataEngine.factory.interfaces' compile
  without source changes.
}
unit DataEngine.factory.interfaces;

interface

uses
  DataEngine.FactoryInterfaces;

implementation

end.

