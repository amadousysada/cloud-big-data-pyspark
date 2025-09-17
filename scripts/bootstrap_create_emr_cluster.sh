#!/usr/bin/env bash
# Usage: ./bootstrap_create_emr_cluster.sh --conf emr.conf.json
set -euo pipefail

info()  { echo -e "ℹ️  $*"; }
warn()  { echo -e "⚠️  $*" >&2; }
error() { echo -e "❌ $*" >&2; exit 64; }

command -v aws >/dev/null 2>&1 || error "AWS CLI introuvable."
command -v jq  >/dev/null 2>&1 || error "jq introuvable (sudo apt-get install jq)."

# ---- Args
CONF_FILE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --conf|--config) CONF_FILE="$2"; shift 2;;
    -h|--help) echo "Usage: $0 --conf emr.conf.json"; exit 0;;
    *) error "Argument inconnu: $1 (utilise --conf emr.conf.json)";;
  esac
done
[[ -n "${CONF_FILE}" ]] || error "--conf <fichier> est obligatoire."
[[ -f "${CONF_FILE}" ]] || error "Fichier introuvable: ${CONF_FILE}"
jq -e . >/dev/null 2>&1 < "${CONF_FILE}" || error "JSON invalide: ${CONF_FILE}"

# ---- Conf + validations
ACCOUNT_ID="$(jq -r '.account_id // empty' "${CONF_FILE}")"
[[ -n "${ACCOUNT_ID}" ]] || error "account_id manquant dans la conf."
[[ "${ACCOUNT_ID}" =~ ^[0-9]{12}$ ]] || error "account_id doit faire 12 chiffres. Reçu: '${ACCOUNT_ID}'"

CLUSTER_NAME="$(jq -r '.cluster_name // empty' "${CONF_FILE}")"
[[ -n "${CLUSTER_NAME}" ]] || { CLUSTER_NAME="p11-fruits-pipeline-eu"; warn "cluster_name absent -> '${CLUSTER_NAME}'"; }

REGION="$(jq -r '.region // empty' "${CONF_FILE}")"
[[ -n "${REGION}" ]] || { REGION="eu-west-3"; warn "region absente -> '${REGION}'"; }

RELEASE_LABEL="$(jq -r '.release_label // empty' "${CONF_FILE}")"
[[ -n "${RELEASE_LABEL}" ]] || { RELEASE_LABEL="emr-7.10.0"; warn "release_label absent -> '${RELEASE_LABEL}'"; }

# ---- Logging
LOG_BUCKET="$(jq -r '.logging.log_bucket // empty' "${CONF_FILE}")"
LOG_PREFIX="$(jq -r '.logging.log_prefix // empty' "${CONF_FILE}")"
[[ -n "${LOG_BUCKET}" ]] || { LOG_BUCKET="aws-logs-${ACCOUNT_ID}-${REGION}"; warn "logging.log_bucket absent -> '${LOG_BUCKET}'"; }
[[ -n "${LOG_PREFIX}" ]] || LOG_PREFIX="elasticmapreduce/"
LOG_URI="s3://${LOG_BUCKET}/${LOG_PREFIX}"

# Auto-création optionnelle du bucket de logs
if ! aws s3api head-bucket --bucket "${LOG_BUCKET}" >/dev/null 2>&1; then
  warn "Bucket logs '${LOG_BUCKET}' introuvable. Création en cours…"
  aws s3api create-bucket --bucket "${LOG_BUCKET}" --region "${REGION}" \
    --create-bucket-configuration "LocationConstraint=${REGION}" >/dev/null
  aws s3api put-bucket-encryption --bucket "${LOG_BUCKET}" \
    --server-side-encryption-configuration '{
      "Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]
    }' >/dev/null || true
  aws s3api put-public-access-block --bucket "${LOG_BUCKET}" \
    --public-access-block-configuration '{
      "BlockPublicAcls":true,"IgnorePublicAcls":true,
      "BlockPublicPolicy":true,"RestrictPublicBuckets":true
    }' >/dev/null || true
  info "Bucket de logs '${LOG_BUCKET}' créé."
fi

# ---- IAM
SERVICE_ROLE_NAME="$(jq -r '.iam.service_role // empty' "${CONF_FILE}")"
SERVICE_ROLE_ARN="$(jq -r '.iam.service_role_arn // empty' "${CONF_FILE}")"
INSTANCE_PROFILE="$(jq -r '.iam.instance_profile // empty' "${CONF_FILE}")"

if [[ -n "${SERVICE_ROLE_ARN}" && -z "${SERVICE_ROLE_NAME}" ]]; then
  SERVICE_ROLE_NAME="${SERVICE_ROLE_ARN##*/}"
fi
[[ -n "${SERVICE_ROLE_NAME}" ]] || { SERVICE_ROLE_NAME="EMR_DefaultRole"; warn "iam.service_role absent -> '${SERVICE_ROLE_NAME}'"; }
[[ -n "${INSTANCE_PROFILE}"   ]] || { INSTANCE_PROFILE="EMR_EC2_DefaultRole"; warn "iam.instance_profile absent -> '${INSTANCE_PROFILE}'"; }

aws iam get-role --role-name "${SERVICE_ROLE_NAME}" >/dev/null 2>&1 \
  || error "Service role '${SERVICE_ROLE_NAME}' introuvable. Tip: aws emr create-default-roles"
aws iam get-instance-profile --instance-profile-name "${INSTANCE_PROFILE##*/}" >/dev/null 2>&1 \
  || error "Instance profile '${INSTANCE_PROFILE}' introuvable. Tip: aws emr create-default-roles"

if [[ "${INSTANCE_PROFILE}" =~ ^arn:aws:iam::[0-9]{12}:instance-profile/ ]]; then
  INSTANCE_PROFILE_ARN="${INSTANCE_PROFILE}"
else
  INSTANCE_PROFILE_ARN="arn:aws:iam::${ACCOUNT_ID}:instance-profile/${INSTANCE_PROFILE}"
fi

# ---- EC2 attributes (toujours avec InstanceProfile)
EC2_ATTR_JSON="$(jq -c --arg ip "${INSTANCE_PROFILE_ARN}" '
  .network as $n |
  (if ($n|type)=="object" then {
      SubnetIds: ($n.subnet_ids // []),
      KeyName:   ($n.key_name // null),
      EmrManagedMasterSecurityGroup: ($n.master_sg // null),
      EmrManagedSlaveSecurityGroup:  ($n.core_sg // null),
      AdditionalMasterSecurityGroups: ($n.additional_master_sg // []),
      AdditionalSlaveSecurityGroups:  ($n.additional_core_sg // [])
    } else {} end)
  | with_entries(
      select(.value != null and ( ( (.value|type)=="array" and (.value|length)>0 ) or (.value|type)!="array"))
    )
  | . + { InstanceProfile: $ip }
' "${CONF_FILE}")"

# ---- Applications (shorthand Name=… tokens)
readarray -t APPS < <(jq -r '.applications[]? // empty' "${CONF_FILE}")
if [[ ${#APPS[@]} -eq 0 ]]; then
  warn "applications absentes -> fallback ['Spark','Livy','JupyterEnterpriseGateway']"
  APPS=(Spark Livy JupyterEnterpriseGateway)
fi
APPLICATIONS_ARGS=()
for a in "${APPS[@]}"; do APPLICATIONS_ARGS+=( "Name=${a}" ); done

# ---- Tags (EMR attend key=value, PAS JSON)
readarray -t TAG_KV < <(jq -r '.tags | to_entries[]? | "\(.key)=\(.value)"' "${CONF_FILE}")
if [[ ${#TAG_KV[@]} -eq 0 ]]; then
  TAG_KV=("project=p11_cluster")
  warn "tags absents -> fallback {project=p11_cluster}"
fi
TAGS_ARGS=("${TAG_KV[@]}")

# ---- Instance groups (JSON compact)
INSTANCE_GROUPS_JSON="$(jq -c '
  (.instance_groups // [
    { "type":"MASTER","name":"Primary","instance_type":"m5.xlarge","count":1,
      "ebs":{"volume_type":"gp3","size_gb":32,"volumes_per_instance":2} },
    { "type":"CORE","name":"Core","instance_type":"m5.xlarge","count":2,
      "ebs":{"volume_type":"gp3","size_gb":32,"volumes_per_instance":2} }
  ])
  | map({
      InstanceCount: (.count // 1),
      InstanceGroupType: ((.type // "CORE") | ascii_upcase),
      Name: (.name // (.type // "CORE")),
      InstanceType: (.instance_type // "m5.xlarge"),
      EbsConfiguration: { EbsBlockDeviceConfigs: [
        { VolumeSpecification: { VolumeType: ((.ebs.volume_type)//"gp3"), SizeInGB: ((.ebs.size_gb)//32) },
          VolumesPerInstance: ((.ebs.volumes_per_instance)//1) }
      ]}
    })
' "${CONF_FILE}")"

# ---- Bootstrap actions (nouveau)
BOOTSTRAP_JSON="$(jq -c '
  (.bootstrap_actions // [])
  | map(select((.path // "") != ""))
  | map({
      Path: .path,
      Name: (.name // "bootstrap"),
      Args: (.args // [])
    } | with_entries(
        select(.value != null and ( ( (.value|type)=="array" and (.value|length)>0 ) or (.value|type)!="array"))
      )
    )
' "${CONF_FILE}")"

# ---- Divers
UNHEALTHY_REPLACE="$(jq -r '.enable_unhealthy_node_replacement // true' "${CONF_FILE}")"
SCALE_DOWN="$(jq -r '.scale_down_behavior // "TERMINATE_AT_TASK_COMPLETION"' "${CONF_FILE}")"
IDLE_SEC="$(jq -r '.auto_termination_idle_seconds // 3600' "${CONF_FILE}")"

# ---- Récap
info "Cluster:        ${CLUSTER_NAME}"
info "Account ID:     ${ACCOUNT_ID}"
info "Region:         ${REGION}"
info "Release:        ${RELEASE_LABEL}"
info "Log URI:        ${LOG_URI}"
info "Service Role:   ${SERVICE_ROLE_NAME}"
info "InstanceProfile:${INSTANCE_PROFILE}"
info "Apps:           ${APPS[*]}"
info "Tags:           $(printf "%s " "${TAGS_ARGS[@]}")"
info "EC2 Attr:       ${EC2_ATTR_JSON}"
info "InstanceGroups: ${INSTANCE_GROUPS_JSON}"
if [[ -n "${BOOTSTRAP_JSON}" && "${BOOTSTRAP_JSON}" != "[]" && "${BOOTSTRAP_JSON}" != "null" ]]; then
  info "Bootstrap:      ${BOOTSTRAP_JSON}"
else
  warn "Aucune bootstrap action configurée."
fi
info "ScaleDown:      ${SCALE_DOWN}"
info "IdleTerminate:  ${IDLE_SEC}s"
[[ "${UNHEALTHY_REPLACE}" == "true" ]] && info "Unhealthy node replacement: ENABLED" || info "Unhealthy node replacement: DISABLED"

# ---- Commande
CMD=( aws emr create-cluster
  --name "${CLUSTER_NAME}"
  --log-uri "${LOG_URI}"
  --release-label "${RELEASE_LABEL}"
  --service-role "${SERVICE_ROLE_NAME}"
  --applications "${APPLICATIONS_ARGS[@]}"
  --tags "${TAGS_ARGS[@]}"
  --instance-groups "${INSTANCE_GROUPS_JSON}"
  --scale-down-behavior "${SCALE_DOWN}"
  --auto-termination-policy "{\"IdleTimeout\":${IDLE_SEC}}"
  --ec2-attributes "${EC2_ATTR_JSON}"
  --region "${REGION}"
)
[[ "${UNHEALTHY_REPLACE}" == "true" ]] && CMD+=( --unhealthy-node-replacement )
# -> ajout bootstrap
if [[ -n "${BOOTSTRAP_JSON}" && "${BOOTSTRAP_JSON}" != "[]" && "${BOOTSTRAP_JSON}" != "null" ]]; then
  CMD+=( --bootstrap-actions "${BOOTSTRAP_JSON}" )
fi

info "-> Lancement du cluster EMR…"
"${CMD[@]}"
info "Commande envoyée. Surveille l'état du cluster dans la console EMR."
