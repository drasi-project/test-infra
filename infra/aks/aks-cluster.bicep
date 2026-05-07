@description('The name of the AKS cluster')
param clusterName string

@description('The location of the AKS cluster')
param location string = 'westus3'

@description('Optional DNS prefix to use with hosted Kubernetes API server FQDN')
param dnsPrefix string = 'drasi-aks-${uniqueString(resourceGroup().id)}'

@description('The number of nodes for the system pool')
param systemNodeCount int = 2

@description('The size of the Virtual Machine')
param vmSize string = 'Standard_D8ds_v5'

resource aks 'Microsoft.ContainerService/managedClusters@2024-02-01' = {
  name: clusterName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    dnsPrefix: dnsPrefix
    autoUpgradeProfile: {
      nodeOSUpgradeChannel: 'NodeImage'
      upgradeChannel: 'stable'
    }
    agentPoolProfiles: [
      {
        name: 'agentpool'
        count: systemNodeCount
        vmSize: vmSize
        osType: 'Linux'
        osSKU: 'AzureLinux'
        mode: 'System'
        type: 'VirtualMachineScaleSets'
      }
    ]
  }
}

output controlPlaneFQDN string = aks.properties.fqdn
