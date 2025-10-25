import os
import yaml
import secrets
from kubernetes import client, config, stream
from kubernetes.utils import create_from_yaml
import logging
from pathlib import Path
import hashlib
import tarfile
import string
import subprocess
import pystache
import base64
import shutil
import io
import time
from multiprocessing import Pool
import requests
import argparse

def reset_proxy():
    os.environ['HTTP_PROXY'] = ''
    os.environ['HTTPS_PROXY'] = ''
    os.environ['NO_PROXY'] = ''
    os.environ['http_proxy'] = ''
    os.environ['https_proxy'] = ''
    os.environ['no_proxy'] = ''


def get_default_password():
    try:
        return os.environ['ELASTIC_PASSWORD']
    except:
        raise RuntimeError("Impossible de déterminer le mot de passe du compte elastic d'ElasticSearch")


def get_default_config_folder():
    dossiers_candidats = [f for f in Path.home().rglob("*") if (f.is_file() and f.name == 'config-cluster.yml')]
    if len(dossiers_candidats) == 1:
        return str(dossiers_candidats[0].parent.resolve())
    else:
        raise RuntimeError("Impossible de déterminer le dossier de config par défaut")


def get_default_namespace():
    try:
        return os.environ['KUBERNETES_NAMESPACE']
    except:
        raise RuntimeError("Impossible de déterminer le namespace Kubernetes")


def copy_folder_to_pod(
        namespace: str,
        pod_name: str,
        container_name: str,
        local_folder: str,
        remote_path: str,
        client,
        chunk_size: int = 1024 * 1024):
    core_v1 = client.CoreV1Api()

    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode='w') as tar:
        tar.add(local_folder, arcname=os.path.basename(local_folder))
    tar_stream.seek(0)  # rewind

    exec_command = ['tar', 'xf', '-', '-C', remote_path]
    ws = stream.stream(
        core_v1.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        container=container_name,
        command=exec_command,
        stderr=True,
        stdin=True,
        stdout=True,
        tty=False,
        _preload_content=False
    )

    while ws.is_open():
        ws.update(timeout=1)

        out = ws.read_stdout(timeout=0)
        if out:
            print("STDOUT:", out)

        err = ws.read_stderr(timeout=0)
        if err:
            print("STDERR:", err)

        chunk = tar_stream.read(chunk_size)
        if chunk:
            ws.write_stdin(chunk)
        else:
            break

    ws.run_forever(timeout=5)
    ws.close()

def list_statefulset_containers(namespace: str, statefulset_name: str, client):
    # Load config (adapt as needed)
    apps_v1 = client.AppsV1Api()
    core_v1 = client.CoreV1Api()

    # 1. Get the StatefulSet
    sts = apps_v1.read_namespaced_stateful_set(name=statefulset_name, namespace=namespace)
    selector = sts.spec.selector.match_labels 
    selector_str = ",".join(f"{k}={v}" for k,v in selector.items())
    pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=selector_str)
    output = []
    for pod in pods.items:
        ready = False
        cs_name = None
        if pod.status.container_statuses:
            for cs in pod.status.container_statuses:
                if not ready:
                    ready=cs.ready
                    cs_name = cs.name
        output.append((pod.metadata.name, cs_name, ready))
    return output

def wait_pod_ready(namespace: str, statefulset_name: str, client, timeout=60, interval=1):
    start = time.time()
    while True:
        try:
            elements = list_statefulset_containers(namespace= namespace, statefulset_name= statefulset_name, client= client)
            if len(elements) > 0:
                for el in elements:
                    if el[2]:
                        return el
        except:
            pass
        if time.time() - start > timeout:
            raise TimeoutError(f"Timeout after {timeout}s: condition not met.")

        time.sleep(interval)

def str_to_base64(s: str) -> str:
    b = s.encode('utf-8')
    b64_bytes = base64.b64encode(b)
    b64_str = b64_bytes.decode('utf-8')
    return b64_str

def sha512_file(filename: Path):
    sha512 = hashlib.sha512()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha512.update(chunk)
    return sha512.hexdigest()


def verify_checksums(directory: Path, checksum_file: str):
    all_ok = True
    with open(directory / checksum_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            expected, filename = line.split(maxsplit=1)
            actual = sha512_file(directory / filename)
            if actual.lower() != expected.lower():
                all_ok = False
    return all_ok


def generate_password(length=16):
    characters = string.ascii_letters + string.digits
    password = ''.join(secrets.choice(characters) for _ in range(length))
    return password

def download_file(url: str, save_dir: Path, filename: str) -> None:
    # Crée le dossier s’il n’existe pas
    os.makedirs(save_dir, exist_ok=True)
    
    if filename is None:
        # Si aucun nom de fichier fourni, on prend le dernier segment de l’URL
        filename = url.split("/")[-1]
    
    save_path = os.path.join(save_dir, filename)
    
    # Télécharger en streaming (utile si le fichier est gros) 
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(save_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filtre les morceaux vides
                    f.write(chunk)
    return None


def write_from_mustache(
    template: Path,
    output: Path,
    variables: dict[str, str]
):
    template.parent.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)

    with open(template, "r") as f:
        template_content = f.read()

    rendered = pystache.render(template_content, variables)

    with open(output, "w") as f:
        f.write(rendered)


def prepare_elastic(tache):
    i = tache[0]
    home = tache[1]
    dict_config = tache[2]

    reset_proxy()

    try:
        config.load_kube_config()
    except Exception as e:
        logger.error("Impossible de récupérer la configuration du cluster Kubernetes")
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Impossible de récupérer la configuration du cluster Kubernetes") from e

    # Create Kubernetes resources for this node
    logger.info("Application des manifests Kubernetes pour le node {i}: secrets, service, ingress, statefulset".format(i=str(i)))
    with open(home / 'template_kube' / ('node_' + str(i)) / 'secrets.yml') as f:
        doc = yaml.safe_load(f)
    res = create_from_yaml(client.ApiClient(), yaml_objects=[doc], namespace=dict_config['kubernetes_namespace'])
    if not res:
        logger.error("create_from_yaml returned no result for secrets of node {i}".format(i=str(i)))
        raise RuntimeError("Echec application manifest secrets pour node {i}".format(i=str(i)))
    logger.info("Secret appliqué pour le node {i}".format(i=str(i)))

    with open(home / 'template_kube' / ('node_' + str(i)) / 'service.yml') as f:
        doc = yaml.safe_load(f)
    res = create_from_yaml(client.ApiClient(), yaml_objects=[doc], namespace=dict_config['kubernetes_namespace'])
    if not res:
        logger.error("create_from_yaml returned no result for service of node {i}".format(i=str(i)))
        raise RuntimeError("Echec application manifest service pour node {i}".format(i=str(i)))
    logger.info("Service appliqué pour le node {i}".format(i=str(i)))

    with open(home / 'template_kube' / ('node_' + str(i)) / 'ingress.yml') as f:
        doc = yaml.safe_load(f)
    res = create_from_yaml(client.ApiClient(), yaml_objects=[doc], namespace=dict_config['kubernetes_namespace'])
    if not res:
        logger.error("create_from_yaml returned no result for ingress of node {i}".format(i=str(i)))
        raise RuntimeError("Echec application manifest ingress pour node {i}".format(i=str(i)))
    logger.info("Ingress appliqué pour le node {i}".format(i=str(i)))

    with open(home / 'template_kube' / ('node_' + str(i)) / 'StatefulSet.yml') as f:
        doc = yaml.safe_load(f)
    res = create_from_yaml(client.ApiClient(), yaml_objects=[doc], namespace=dict_config['kubernetes_namespace'])
    if not res:
        logger.error("create_from_yaml returned no result for statefulset of node {i}".format(i=str(i)))
        raise RuntimeError("Echec application manifest statefulset pour node {i}".format(i=str(i)))
    logger.info("StatefulSet appliqué pour le node {i}".format(i=str(i)))

    logger.info("Début de la création du Pod pour le noeud {i}".format(i=str(i)))
    res = wait_pod_ready(namespace= dict_config['kubernetes_namespace'], statefulset_name= (dict_config['cluster_name'] + '-node-' + str(i)) , client = client, timeout=60, interval=1)
    logger.info("Pod prêt pour le noeud {i} : pod={pod}, container={container}".format(i=str(i), pod=res[0], container=res[1]))

    def _check_stream_response(resp, context, check_empty: bool = False, allow_empty: bool = False):
        out = resp if isinstance(resp, str) else (resp.decode() if isinstance(resp, bytes) else str(resp))
        out_str = out.strip()
        logger.info("{ctx} output: {o}".format(ctx=context, o=out_str[:1000]))  # truncate long output
        low = out_str.lower()
        if check_empty and out_str:
            logger.error("{ctx} returned non-mpty output".format(ctx=context))
            raise RuntimeError("{ctx} failed: non-empty output".format(ctx=context))
        if not allow_empty and not out_str:
            logger.error("{ctx} returned empty output".format(ctx=context))
            raise RuntimeError("{ctx} failed: empty output".format(ctx=context))
        if any(k in low for k in ("error", "failed", "no such", "not found", "permission denied")):
            logger.error("{ctx} reported error: {o}".format(ctx=context, o=out_str))
            raise RuntimeError("{ctx} failed: {o}".format(ctx=context, o=out_str))
        return out_str

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Téléchargement d'ElasticSearch dans le pod {pod} (node {i})".format(pod=res[0], i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['wget', '-O', '/home/onyxia/work/bin/elasticsearch-{version}-linux-x86_64.tar.gz'.format(version=dict_config['versionElastic']), '{prefix}/elasticsearch/elasticsearch-{version}-linux-x86_64.tar.gz'.format(prefix=dict_config['downloadPrefix'], version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "wget in pod node {i}".format(i=str(i)))
        logger.info("Téléchargement dans le pod terminé pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant le téléchargement dans le pod node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Extraction de l'archive ElasticSearch dans le pod {pod} (node {i})".format(pod=res[0], i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['/bin/sh', '-c', "cd /home/onyxia/work/bin && tar -xzf /home/onyxia/work/bin/elasticsearch-{version}-linux-x86_64.tar.gz".format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "tar extract in pod node {i}".format(i=str(i)), True, True)
        logger.info("Extraction terminée dans le pod pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant l'extraction dans le pod node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Suppression de l'archive dans le pod {pod} pour node {i}".format(pod=res[0], i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['rm', '/home/onyxia/work/bin/elasticsearch-{version}-linux-x86_64.tar.gz'.format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "rm archive in pod node {i}".format(i=str(i)), True, True)
        logger.info("Archive supprimée dans le pod pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la suppression de l'archive dans le pod node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        logger.info("Copie des fichiers locaux vers le pod {pod} pour node {i}".format(pod=res[0], i=str(i)))
        copy_folder_to_pod(
            namespace= dict_config['kubernetes_namespace'],
            pod_name= res[0],
            container_name= res[1],
            local_folder= str((home / 'export' / ('node_' + str(i))).resolve()),
            remote_path= '/home/onyxia/work',
            client = client
        )
        logger.info("Copie des fichiers vers le pod terminée pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la copie des fichiers vers le pod node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Création du dossier certs dans l'installation ElasticSearch du pod {pod} pour node {i}".format(pod=res[0], i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['mkdir', '/home/onyxia/work/bin/elasticsearch-{version}/config/certs'.format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "mkdir certs in pod node {i}".format(i=str(i)), True, True)
        logger.info("Dossier certs créé dans le pod pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la création du dossier certs dans le pod node {i}: {err}".format(i=str(i), err=e))
        raise

    # Copy configuration and keystores into the elastic installation in the pod
    try:
        core_v1 = client.CoreV1Api()
        logger.info("Copie de elasticsearch.yml dans l'installation ElasticSearch du pod {pod} pour node {i}".format(pod=res[0], i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ["cp", "/home/onyxia/work/node_{i}/elasticsearch.yml".format(i=str(i)), "/home/onyxia/work/bin/elasticsearch-{version}/config/elasticsearch.yml".format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "cp elasticsearch.yml in pod node {i}".format(i=str(i)), True, True)
        logger.info("elasticsearch.yml copié dans le pod pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la copie de elasticsearch.yml dans le pod node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Copie du keystore transport pour node {i} dans le pod {pod}".format(i=str(i), pod=res[0]))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ["cp", "/home/onyxia/work/node_{i}/transport-node-{i}.p12".format(i=str(i)), "/home/onyxia/work/bin/elasticsearch-{version}/config/certs/transport-node-{i}.p12".format(i=str(i), version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "cp transport keystore in pod node {i}".format(i=str(i)), True, True)
        logger.info("Keystore transport copié pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la copie du keystore transport pour node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Copie du keystore http pour node {i} dans le pod {pod}".format(i=str(i), pod=res[0]))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ["cp", "/home/onyxia/work/node_{i}/http-node-{i}.p12".format(i=str(i)), "/home/onyxia/work/bin/elasticsearch-{version}/config/certs/http-node-{i}.p12".format(i=str(i), version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "cp http keystore in pod node {i}".format(i=str(i)), True, True)
        logger.info("Keystore http copié pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la copie du keystore http pour node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Copie du keystore CA (p12) pour node {i} dans le pod {pod}".format(i=str(i), pod=res[0]))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ["cp", "/home/onyxia/work/node_{i}/elastic-stack-ca.p12".format(i=str(i)), "/home/onyxia/work/bin/elasticsearch-{version}/config/certs/elastic-stack-ca.p12".format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "cp ca p12 in pod node {i}".format(i=str(i)), True, True)
        logger.info("CA keystore (p12) copié pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la copie du CA keystore pour node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Copie du CA crt pour node {i} dans le pod {pod}".format(i=str(i), pod=res[0]))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ["cp", "/home/onyxia/work/node_{i}/elastic-stack-ca.crt".format(i=str(i)), "/home/onyxia/work/bin/elasticsearch-{version}/config/certs/elastic-stack-ca.crt".format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "cp ca crt in pod node {i}".format(i=str(i)), True, True)
        logger.info("CA crt copié pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la copie du CA crt pour node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Suppression du dossier temporaire node_{i} dans le pod {pod}".format(i=str(i), pod=res[0]))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['rm', '-R', '/home/onyxia/work/node_{i}'.format(i=str(i))],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "rm temp node dir in pod node {i}".format(i=str(i)), True, True)
        logger.info("Dossier temporaire supprimé dans le pod pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la suppression du dossier temporaire dans le pod node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Création du elasticsearch-keystore dans le pod {pod} pour node {i}".format(pod=res[0], i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['/home/onyxia/work/bin/elasticsearch-{version}/bin/elasticsearch-keystore'.format(version=dict_config['versionElastic']), 'create'],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "create elasticsearch-keystore in pod node {i}".format(i=str(i)))
        logger.info("Elasticsearch keystore créé pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant la création du keystore pour node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Ajout du bootstrap.password dans le keystore pour node {i}".format(i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['/bin/sh', '-c', 'echo -n $ELASTIC_TRUSTORE_PASSWORD | /home/onyxia/work/bin/elasticsearch-{version}/bin/elasticsearch-keystore add bootstrap.password'.format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "add bootstrap.password in pod node {i}".format(i=str(i)))
        logger.info("bootstrap.password ajouté au keystore pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant l'ajout de bootstrap.password pour node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Ajout des mots de passe transport keystore dans le keystore pour node {i}".format(i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['/bin/sh', '-c', 'echo -n $ELASTIC_TRANSPORT_KEYSTORE_PASSWORD | /home/onyxia/work/bin/elasticsearch-{version}/bin/elasticsearch-keystore add xpack.security.transport.ssl.keystore.secure_password'.format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "add transport keystore pwd in pod node {i}".format(i=str(i)))
        logger.info("Password transport keystore ajouté pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant l'ajout du password transport keystore pour node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Ajout du password pour truststore dans le keystore pour node {i}".format(i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['/bin/sh', '-c', 'echo -n $ELASTIC_TRANSPORT_KEYSTORE_PASSWORD | /home/onyxia/work/bin/elasticsearch-{version}/bin/elasticsearch-keystore add xpack.security.transport.ssl.truststore.secure_password'.format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "add truststore pwd in pod node {i}".format(i=str(i)))
        logger.info("Password truststore ajouté pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant l'ajout du truststore password pour node {i}: {err}".format(i=str(i), err=e))
        raise

    try:
        core_v1 = client.CoreV1Api()
        logger.info("Ajout du password http keystore dans le keystore pour node {i}".format(i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['/bin/sh', '-c', 'echo -n $ELASTIC_HTTP_KEYSTORE_PASSWORD | /home/onyxia/work/bin/elasticsearch-{version}/bin/elasticsearch-keystore add xpack.security.http.ssl.keystore.secure_password'.format(version=dict_config['versionElastic'])],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        _check_stream_response(resp, "add http keystore pwd in pod node {i}".format(i=str(i)))
        logger.info("Password http keystore ajouté pour node {i}".format(i=str(i)))
    except Exception as e:
        logger.error("Erreur pendant l'ajout du http keystore password pour node {i}: {err}".format(i=str(i), err=e))
        raise

def run_elastic(tache):
    try:
        i = tache[0]
        dict_config = tache[1]
        version = dict_config['versionElastic']
        res = wait_pod_ready(namespace= dict_config['kubernetes_namespace'], statefulset_name= (dict_config['cluster_name'] + '-node-' + str(i)) , client = client, timeout=60, interval=1)
        core_v1 = client.CoreV1Api()
        logger.info("Démarrage d'elasticsearch dans le pod {pod} (node {i})".format(pod=res[0], i=str(i)))
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            res[0],
            dict_config['kubernetes_namespace'],
            command= ['/home/onyxia/work/bin/elasticsearch-{version}/bin/elasticsearch'.format(version=version), '-d', '-p' ,'pid'],
            container=res[1],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        # check start output
        out = resp if isinstance(resp, str) else (resp.decode() if isinstance(resp, bytes) else str(resp))
        logger.info("Start elastic output (truncated): {o}".format(o=out.strip()[:1000]))
        if not out.strip() or any(k in out.lower() for k in ("error","failed","permission denied","cannot")):
            logger.error("Elasticsearch start failed for node {i}: {o}".format(i=str(i), o=out))
            raise RuntimeError("Echec démarrage elasticsearch pour node {i}".format(i=str(i)))
        return resp
    except Exception as e:
        raise


parser = argparse.ArgumentParser(description="Script d'initialisation d'un cluster ElasticSearch sur un cluster Kubernetes")
parser.add_argument("--password", type=str, help="Mot de passe pour l'user elastic du cluster", default=get_default_password())
parser.add_argument("--config", type=str, help="Chemin vers le dossier de configuration", default=get_default_config_folder())
parser.add_argument("--namespace", type=str, help="Nom du namespace Kubernetes", default=get_default_namespace())

args = parser.parse_args()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

logger.info("Démarrage du cluster Elasticsearch")
logger.info("Téléchargement de l'archive")

home = Path.home()
elastic_download_folder=home / 'elastic'

dict_config: dict[str, str] = {
    'config_directory' : args.config,
    'kubernetes_namespace': args.namespace,
    'elastic_password': args.password,
    'elastic_transport_keystore_password_1': generate_password(25),
    'elastic_http_keystore_password_1': generate_password(25),
    'elastic_transport_keystore_password_2': generate_password(25),
    'elastic_http_keystore_password_2': generate_password(25),
    'elastic_transport_keystore_password_3': generate_password(25),
    'elastic_http_keystore_password_3': generate_password(25)
}


yaml_file = Path(dict_config['config_directory']) / 'config-cluster.yml'

try:
    with open(yaml_file) as f:
        data_config_cluster = yaml.safe_load(f)
except Exception as e:
    logger.error("Impossible de charger le fichier yaml de configuration du cluster")
    raise RuntimeError("Impossible de charger le fichier yaml de configuration du cluster") from e

if 'clusterName' not in data_config_cluster:
    raise RuntimeError("Le nom du cluster n'est pas configuré")
if not isinstance(data_config_cluster['clusterName'], str):
    raise RuntimeError("Le nom du cluster n'est pas configuré")

if 'downloadPrefix' not in data_config_cluster:
    raise RuntimeError("Le prefixe de téléchargement de l'archive n'est pas configuré")
if not isinstance(data_config_cluster['downloadPrefix'], str):
    raise RuntimeError("Le nom du cluster n'est pas configuré")

if 'versionElastic' not in data_config_cluster:
    raise RuntimeError("La version d'elastic n'est pas configurée")
if not isinstance(data_config_cluster['versionElastic'], str):
    raise RuntimeError("La version d'elastic n'est pas configurée")

if 'versionKibana' not in data_config_cluster:
    raise RuntimeError("La version de Kibana n'est pas configurée")
if not isinstance(data_config_cluster['versionKibana'], str):
    raise RuntimeError("La version de Kibana n'est pas configurée")

if 'ingressDomain' not in data_config_cluster:
    raise RuntimeError("Le nom de domaine pour l'ingress n'est pas configuré")
if not isinstance(data_config_cluster['ingressDomain'], str):
    raise RuntimeError("Le nom de domaine pour l'ingress n'est pas configuré")

if 'image' not in data_config_cluster:
    raise RuntimeError("Le nom de l'image docker n'est pas configuré")
if not isinstance(data_config_cluster['image'], str):
    raise RuntimeError("Le nom de l'image docker n'est pas configuré")


if 'limits' not in data_config_cluster:
    raise RuntimeError("L'argument limits du cluster n'est pas configuré")
if not isinstance(data_config_cluster['limits'], dict):
    raise RuntimeError("L'argument limits du cluster n'est pas configuré")

if 'cpu' not in data_config_cluster['limits']:
    raise RuntimeError("L'argument cpu du cluster n'est pas configuré")
if not isinstance(data_config_cluster['limits']['cpu'], str):
    raise RuntimeError("L'argument cpu du cluster n'est pas configuré")

if 'memory' not in data_config_cluster['limits']:
    raise RuntimeError("L'argument memory du cluster n'est pas configuré")
if not isinstance(data_config_cluster['limits']['memory'], str):
    raise RuntimeError("L'argument memory du cluster n'est pas configuré")

if 'persistentStorage' not in data_config_cluster['limits']:
    raise RuntimeError("L'argument persistentStorage du cluster n'est pas configuré")
if not isinstance(data_config_cluster['limits']['persistentStorage'], str):
    raise RuntimeError("L'argument persistentStorage du cluster n'est pas configuré")

dict_config['cluster_name'] = data_config_cluster['clusterName']
dict_config['cpu'] = data_config_cluster['limits']['cpu']
dict_config['memory'] = data_config_cluster['limits']['memory']
dict_config['persistent_storage'] = data_config_cluster['limits']['persistentStorage']
dict_config['downloadPrefix'] = data_config_cluster['downloadPrefix']
dict_config['versionElastic'] = data_config_cluster['versionElastic']
dict_config['versionKibana'] = data_config_cluster['versionKibana']
dict_config['ingressDomain'] = data_config_cluster['ingressDomain']
dict_config['image'] = data_config_cluster['image']

try:
    logger.info("Création du dossier {folder}".format(folder=elastic_download_folder.resolve()))
    elastic_download_folder.mkdir(parents=True, exist_ok=True)
    logger.info("Dossier de téléchargement créé: {folder}".format(folder=elastic_download_folder.resolve()))
except Exception as e:
    logger.error("Impossible de créer le dossier {folder}".format(folder=elastic_download_folder.resolve()))
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Erreur lors de la création d'un dossier") from e

try:
    logger.info("Téléchargement de ElasticSearch version {version} depuis {prefix}".format(version=dict_config['versionElastic'], prefix=dict_config['downloadPrefix']))
    download_file(
        url="{prefix}/elasticsearch/elasticsearch-{version}-linux-x86_64.tar.gz".format(
            prefix=dict_config['downloadPrefix'],
            version=dict_config['versionElastic']
        ),
        save_dir=elastic_download_folder,
        filename='elasticsearch-{version}-linux-x86_64.tar.gz'.format(version=dict_config['versionElastic'])
    )
    logger.info("Téléchargement terminé: elasticsearch-{version}-linux-x86_64.tar.gz".format(version=dict_config['versionElastic']))
except Exception as e:
    logger.error("Impossible de télécharger l'archive ElasticSearch")
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Impossible de télécharger l'archive ElasticSearch ") from e

try:
    logger.info("Téléchargement du fichier de checksum pour ElasticSearch")
    download_file(
        url="{prefix}/elasticsearch/elasticsearch-{version}-linux-x86_64.tar.gz.sha512".format(
            prefix=dict_config['downloadPrefix'],
            version=dict_config['versionElastic']
        ),
        save_dir=elastic_download_folder,
        filename='elasticsearch-{version}-linux-x86_64.tar.gz.sha512'.format(version=dict_config['versionElastic'])
    )
    logger.info("Téléchargement terminé: checksum elasticsearch-{version}-linux-x86_64.tar.gz.sha512".format(version=dict_config['versionElastic']))
except Exception as e:
    logger.error("Impossible de télécharger l'archive ElasticSearch")
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Impossible de télécharger l'archive ElasticSearch") from e

logger.info("Vérification des checksums pour ElasticSearch")
if not verify_checksums(elastic_download_folder, 'elasticsearch-{version}-linux-x86_64.tar.gz.sha512'.format(version=dict_config['versionElastic'])):
    logger.error("Checksum invalide pour ElasticSearch")
    raise RuntimeError("Le fichier téléchargé n'a pas été téléchargé correctement")
logger.info("Checksum OK pour ElasticSearch")

try:
    logger.info("Décompression de l'archive ElasticSearch")
    with tarfile.open(elastic_download_folder / 'elasticsearch-{version}-linux-x86_64.tar.gz'.format(version=dict_config['versionElastic']), "r:gz") as tar:
        tar.extractall(
            path=elastic_download_folder / 'elasticsearch-{version}-linux-x86_64'.format(version=dict_config['versionElastic']),
            filter='fully_trusted'
        )
    logger.info("Décompression ElasticSearch terminée")
except Exception as e:
    logger.error("Impossible de décompresser l'archive ElasticSearch")
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Impossible de décompresser l'archive ElasticSearch") from e


certs_folder= home / 'certs'
try:
    logger.info("Création du dossier {folder}".format(folder=str(certs_folder.resolve())))
    certs_folder.mkdir(parents=True, exist_ok=True)
    logger.info("Dossier certificats créé: {folder}".format(folder=str(certs_folder.resolve())))
except Exception as e:
    logger.error("Impossible de créer le dossier {folder}".format(folder=str(certs_folder.resolve())))
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Erreur lors de la création d'un dossier") from e

certutil_path = elastic_download_folder / 'elasticsearch-{version}-linux-x86_64'.format(version=dict_config['versionElastic']) / 'elasticsearch-{version}'.format(version=dict_config['versionElastic']) / 'bin' / 'elasticsearch-certutil'
output_certif_ca = str((certs_folder / 'elastic-stack-ca.p12').resolve())
logger.info("Génération du certificat CA via elasticsearch-certutil (output={})".format(output_certif_ca))
result = subprocess.run(
    [
        str(certutil_path.resolve()), 
        "ca",
        "--out",
        output_certif_ca,
        "--pass",
        dict_config['elastic_password']
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0 or result.stderr:
    logger.error("Impossible de générer le certificat {certif} - rc:{rc} stderr:{stderr}".format(certif=output_certif_ca, rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Impossible de générer le certificat {certif}".format(certif=output_certif_ca))
logger.info("Certificat CA généré: {certif} stdout:{out}".format(certif=output_certif_ca, out=(result.stdout.strip()[:1000])))

logger.info("Extraction de la clé publique du CA")
result = subprocess.run(
    [
        "openssl", 
        "pkcs12",
        "-in",
        output_certif_ca,
        "-clcerts",
        "-nokeys",
        "-out",
        str((certs_folder / 'elastic-stack-ca.crt').resolve()),
        "-password",
        ('pass:'+dict_config['elastic_password'])
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0 or result.stderr:
    logger.error("Impossible de récupérer la clé publique du certificat {certif} - rc:{rc} stderr:{stderr}".format(certif=output_certif_ca, rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Impossible de récupérer la clé publique du certificat {certif}".format(certif=output_certif_ca))
logger.info("Clé publique CA extraite stdout:{out}".format(out=result.stdout.strip()[:1000]))

logger.info("Extraction de la clé privée du CA")
result = subprocess.run(
    [
        "openssl", 
        "pkcs12",
        "-in",
        output_certif_ca,
        "-nocerts",
        "-nodes",
        "-out",
        str((certs_folder / 'elastic-stack-ca.key').resolve()),
        "-password",
        ('pass:'+dict_config['elastic_password'])
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0 or result.stderr:
    logger.error("Impossible de récupérer la clé privée du certificat {certif} - rc:{rc} stderr:{stderr}".format(certif=output_certif_ca, rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Impossible de récupérer la clé privée du certificat {certif}".format(certif=output_certif_ca))
logger.info("Clé privée CA extraite")

logger.info("Génération de la clé privée TLS pour l'ingress")
result = subprocess.run(
    [
        "openssl", 
        "genrsa",
        "-out",
        str((certs_folder / 'tls.key').resolve()),
        "2048"
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0 or result.stderr:
    logger.error("Impossible de générer la clé privée du certificat TLS de l'Ingress - rc:{rc} stderr:{stderr}".format(rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Impossible de générer la clé privée du certificat TLS de l'Ingress")
logger.info("Clé TLS générée")

logger.info("Création de la CSR TLS et signature par le CA pour l'ingress")
result = subprocess.run(
    [
        "openssl", 
        "req",
        "-new",
        "-key",
        str((certs_folder / 'tls.key').resolve()),
        "-out",
        str((certs_folder / 'tls.crs').resolve()),
        "-subj",
        "/CN={cluster_name}-1.{domain}".format(cluster_name=dict_config['cluster_name'], domain=dict_config['ingressDomain']),
        "-addext",
        "subjectAltName=" + ','.join(['DNS:{cluster_name}-{j}.{domain}'.format(cluster_name=dict_config['cluster_name'], domain=dict_config['ingressDomain'], j=str(j)) for j in range(1,4)])
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0 or result.stderr:
    logger.error("Impossible de créer une requête de signature pour le certificat TLS de l'Ingress - rc:{rc} stderr:{stderr}".format(rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Impossible de créer une requête de signature pour le certificat TLS de l'Ingress")
logger.info("CSR TLS créée")

logger.info("Signature de la CSR TLS avec le CA")
result = subprocess.run(
    [
        "openssl", 
        "x509",
        "-req",
        "-in",
        str((certs_folder / 'tls.crs').resolve()),
        "-CA",
        str((certs_folder / 'elastic-stack-ca.crt').resolve()),
        "-CAkey",
        str((certs_folder / 'elastic-stack-ca.key').resolve()),
        "-CAcreateserial",
        "-out",
        str((certs_folder / 'tls.crt').resolve()),
        "-days",
        "365",
        "-sha256"
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0:
    logger.error("Erreur pendant la signature de la CSR TLS - rc:{rc} stderr:{stderr}".format(rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Erreur pendant la signature de la CSR TLS")
logger.info("Certificat TLS pour ingress généré: {f}".format(f=str((certs_folder / 'tls.crt').resolve())))

for i in range(1,4):
    output_certif = str((certs_folder / ('transport-node-' + str(i) + '.p12')).resolve())
    logger.info("Génération du certificat transport pour le noeud {i} -> {f}".format(i=str(i), f=output_certif))
    result = subprocess.run(
        [
            str(certutil_path.resolve()), 
            "cert",
            "--name",
            dict_config['cluster_name']+'-node-'+str(i),
            "--dns",
            dict_config['cluster_name']+'-node-'+str(i)+'.'+dict_config['kubernetes_namespace']+'.svc.cluster.local',
            "--ca",
            output_certif_ca,
            "--ca-pass",
            dict_config['elastic_password'],
            "--out",
            output_certif,
            "--pass",
            dict_config['elastic_transport_keystore_password_'+str(i)]
        ],    
        capture_output=True, 
        text=True 
    )
    if result.returncode != 0 or result.stderr:
        logger.error("Impossible de générer le certificat {certif} - rc:{rc} stderr:{stderr}".format(certif=output_certif, rc=result.returncode, stderr=result.stderr.strip()))
        raise RuntimeError("Impossible de générer le certificat {certif}".format(certif=output_certif))
    logger.info("Certificat transport généré pour le noeud {i}".format(i=str(i)))

    output_certif = str((certs_folder / ('http-node-' + str(i) + '.p12')).resolve())
    logger.info("Génération du certificat http pour le noeud {i} -> {f}".format(i=str(i), f=output_certif))
    result = subprocess.run(
        [
            str(certutil_path.resolve()), 
            "cert",
            "--name",
            dict_config['cluster_name']+'-node-'+str(i),
            "--dns",
           "{cluster_name}-{i}.{domain}".format(cluster_name=dict_config['cluster_name'], domain=dict_config['ingressDomain'], i=str(i)),
            "--ca",
            output_certif_ca,
            "--ca-pass",
            dict_config['elastic_password'],
            "--out",
            output_certif,
            "--pass",
            dict_config['elastic_http_keystore_password_'+str(i)]
        ],    
        capture_output=True, 
        text=True 
    )
    if result.returncode != 0 or result.stderr:
        logger.error("Impossible de générer le certificat {certif} - rc:{rc} stderr:{stderr}".format(certif=output_certif, rc=result.returncode, stderr=result.stderr.strip()))
        raise RuntimeError("Impossible de générer le certificat {certif}".format(certif=output_certif))
    logger.info("Certificat http généré pour le noeud {i}".format(i=str(i)))

for i in range(1, 4):
    output_pack = home / 'export' / ('node_' + str(i))
    try:
        logger.info("Création du dossier d'export pour node_{i}: {folder}".format(i=str(i), folder=str(output_pack.resolve())))
        output_pack.mkdir(parents=True, exist_ok=True)
        logger.info("Dossier d'export créé: {folder}".format(folder=str(output_pack.resolve())))
    except Exception as e:
        logger.error("Impossible de créer le dossier {folder}".format(folder=str(output_pack.resolve())))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Erreur lors de la création d'un dossier") from e
    
    from_file = Path(dict_config['config_directory']) / 'elasticsearch.yml'
    to_file = output_pack / 'elasticsearch.yml'
    try:
        logger.info("Copie du fichier elasticsearch.yml vers {to}".format(to=str(to_file.resolve())))
        shutil.copyfile(from_file, to_file)
        logger.info("Copie terminée: elasticsearch.yml -> {to}".format(to=str(to_file.resolve())))
    except Exception as e:
        logger.error("Impossible de copier le fichier {f1} à l'emplacement {f2}".format(f1=str(from_file.resolve()), f2=str(to_file.resolve())))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Erreur lors de la copie d'un fichier") from e

    from_file = certs_folder / ('transport-node-' + str(i) + '.p12')
    to_file = output_pack / ('transport-node-' + str(i) + '.p12')
    try:
        logger.info("Copie du keystore transport pour node {i}".format(i=str(i)))
        shutil.copyfile(from_file, to_file)
        logger.info("Copie terminée: {f}".format(f=str(to_file.resolve())))
    except Exception as e:
        logger.error("Impossible de copier le fichier {f1} à l'emplacement {f2}".format(f1=str(from_file.resolve()), f2=str(to_file.resolve())))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Erreur lors de la copie d'un fichier") from e

    from_file = certs_folder / ('http-node-' + str(i) + '.p12')
    to_file = output_pack / ('http-node-' + str(i) + '.p12')
    try:
        logger.info("Copie du keystore http pour node {i}".format(i=str(i)))
        shutil.copyfile(from_file, to_file)
        logger.info("Copie terminée: {f}".format(f=str(to_file.resolve())))
    except Exception as e:
        logger.error("Impossible de copier le fichier {f1} à l'emplacement {f2}".format(f1=str(from_file.resolve()), f2=str(to_file.resolve())))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Erreur lors de la copie d'un fichier") from e

    from_file =   certs_folder /  'elastic-stack-ca.p12'
    to_file = output_pack / 'elastic-stack-ca.p12'
    try:       
        logger.info("Copie du CA keystore vers node_{i}".format(i=str(i)))
        shutil.copyfile(from_file, to_file)
        logger.info("Copie terminée: elastic-stack-ca.p12 -> {to}".format(to=str(to_file.resolve())))
    except Exception as e:
        logger.error("Impossible de copier le fichier {f1} à l'emplacement {f2}".format(f1=str(from_file.resolve()), f2=str(to_file.resolve())))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Erreur lors de la copie d'un fichier") from e

    try:
        from_file =   certs_folder /  'elastic-stack-ca.crt'
        to_file = output_pack / 'elastic-stack-ca.crt'
        logger.info("Copie du CA crt vers node_{i}".format(i=str(i)))
        shutil.copyfile(from_file, to_file)
        logger.info("Copie terminée: elastic-stack-ca.crt -> {to}".format(to=str(to_file.resolve())))
    except Exception as e:
        logger.error("Impossible de copier le fichier {f1} à l'emplacement {f2}".format(f1=str(from_file.resolve()), f2=str(to_file.resolve())))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Erreur lors de la copie d'un fichier") from e

for i in range(1, 4):
    try:
        logger.info("Génération des manifestes Kubernetes pour le node {node}".format(node=str(i)))
        write_from_mustache(
            template=Path(dict_config['config_directory']) / 'StatefulSet.yml',
            output=home / 'template_kube' / ('node_' + str(i)) / 'StatefulSet.yml',
            variables={
                'IMAGE': dict_config['image'],
                'NODE_NAME': (dict_config['cluster_name'] + '-node-' + str(i)),
                'PVC_BIN': (dict_config['cluster_name'] + '-node-' + str(i) + '-bin'),
                'PVC_DATA': (dict_config['cluster_name'] + '-node-' + str(i) + '-data'),
                'PVC_LOGS': (dict_config['cluster_name'] + '-node-' + str(i) + '-logs'),
                'SECRET': (dict_config['cluster_name'] + '-node-' + str(i) + '-secrets'),
                'CPU': dict_config['cpu'],
                'MEMORY': dict_config['memory'],
                'PERSISTENT_VOLUME': dict_config['persistent_storage']
            }
        )
        logger.info("StatefulSet manifest créé pour node {node}".format(node=str(i)))
    except Exception as e:
        logger.error("Impossible de créer le fichier pour le StatefulSet Kubernetes du node {node}".format(node=str(i)))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Impossible de créer le fichier pour le StatefulSet Kubernetes  du node {node}".format(node=str(i))) from e

    try:
        logger.info("Génération du service manifest pour node {node}".format(node=str(i)))
        write_from_mustache(
            template=Path(dict_config['config_directory']) / 'service.yml',
            output=home / 'template_kube' / ('node_' + str(i)) / 'service.yml',
            variables={
                'NODE_NAME': (dict_config['cluster_name'] + '-node-' + str(i))
            }
        )
        logger.info("Service manifest créé pour node {node}".format(node=str(i)))
    except Exception as e:
        logger.error("Impossible de créer le fichier pour le service Kubernetes du node {node}".format(node=str(i)))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Impossible de créer le fichier pour le service Kubernetes  du node {node}".format(node=str(i))) from e

    try:
        logger.info("Génération de l'ingress manifest pour node {node}".format(node=str(i)))
        write_from_mustache(
            template=Path(dict_config['config_directory']) / 'ingress.yml',
            output=home / 'template_kube' / ('node_' + str(i)) / 'ingress.yml',
            variables={
                'NODE_NAME': (dict_config['cluster_name'] + '-node-' + str(i)),
                'CLUSTER_NAME': (dict_config['cluster_name']),
                'DOMAIN_NAME': dict_config['ingressDomain']
            }
        )
        logger.info("Ingress manifest créé pour node {node}".format(node=str(i)))
    except Exception as e:
        logger.error("Impossible de créer le fichier pour le ingress Kubernetes du node {node}".format(node=str(i)))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Impossible de créer le fichier pour le ingress Kubernetes  du node {node}".format(node=str(i))) from e

    try:
        logger.info("Génération des secrets manifest pour node {node}".format(node=str(i)))
        write_from_mustache(
            template=Path(dict_config['config_directory']) / 'secrets.yml',
            output=home / 'template_kube' / ('node_' + str(i)) / 'secrets.yml',
            variables={
                'SECRET': (dict_config['cluster_name'] + '-node-' + str(i) + '-secrets'),
                'CLUSTER_NAME': str_to_base64(dict_config['cluster_name']),
                'NODE_NAME': str_to_base64(dict_config['cluster_name'] + '-node-' + str(i)),
                'ELASTIC_TRANSPORT_KEYSTORE_PATH': str_to_base64('/home/onyxia/work/bin/elasticsearch-{version}/config/certs/transport-node-{i}.p12'.format(i=str(i), version=dict_config['versionElastic'])),
                'ELASTIC_TRANSPORT_KEYSTORE_PASSWORD': str_to_base64(dict_config['elastic_transport_keystore_password_'+str(i)]),
                'ELASTIC_TRUSTORE_PASSWORD': str_to_base64(dict_config['elastic_password']),
                'ELASTIC_HTTP_KEYSTORE_PATH': str_to_base64('/home/onyxia/work/bin/elasticsearch-{version}/config/certs/http-node-{i}.p12'.format(i=str(i), version=dict_config['versionElastic'])),
                'ELASTIC_HTTP_KEYSTORE_PASSWORD': str_to_base64(dict_config['elastic_http_keystore_password_'+str(i)]),
                'ELASTIC_HTTP_CA_PATH': str_to_base64('/home/onyxia/work/bin/elasticsearch-{version}/config/certs/elastic-stack-ca.crt'.format(i=str(i), version=dict_config['versionElastic'])),
                'NODES_LIST': str_to_base64(','.join([(dict_config['cluster_name'] + '-node-' + str(j)) for j in range(1, 4)])),
                'SEED_HOST': str_to_base64(','.join([(dict_config['cluster_name']+'-node-'+str(j)+'.'+ dict_config['kubernetes_namespace']+'.svc.cluster.local:9300') for j in range(1, 4)]))
            }
        )
        logger.info("Secrets manifest créé pour node {node}".format(node=str(i)))
    except Exception as e:
        logger.error("Impossible de créer le fichier pour les secrets Kubernetes du node {node}".format(node=str(i)))
        logger.error(e, stack_info=True, exc_info=True)
        raise RuntimeError("Impossible de créer le fichier pour les secrets Kubernetes  du node {node}".format(node=str(i))) from e

reset_proxy()

try:
    config.load_kube_config()
    logger.info("Configuration Kubernetes chargée pour la création du secret TLS")
except Exception as e:
    logger.error("Impossible de récupérer la configuration du cluster Kubernetes")
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Impossible de récupérer la configuration du cluster Kubernetes") from e

# Read and base64 encode the certificate and key files
with open(str((certs_folder / 'tls.crt').resolve()), "rb") as cert_file:
    tls_crt = base64.b64encode(cert_file.read()).decode("utf-8")

with open(str((certs_folder / 'tls.key').resolve()), "rb") as key_file:
    tls_key = base64.b64encode(key_file.read()).decode("utf-8")

# Create the secret object
secret = client.V1Secret(
    api_version="v1",
    kind="Secret",
    metadata=client.V1ObjectMeta(name=dict_config['cluster_name']+'-tls'),
    type="kubernetes.io/tls",
    data={"tls.crt": tls_crt, "tls.key": tls_key},
)

# Initialize the Kubernetes API client
v1 = client.CoreV1Api()

logger.info("Création du secret TLS kubernetes: {name} dans namespace {ns}".format(name=dict_config['cluster_name']+'-tls', ns=dict_config['kubernetes_namespace']))
v1.create_namespaced_secret(namespace=dict_config['kubernetes_namespace'], body=secret)
logger.info("Secret TLS créé: {name}".format(name=dict_config['cluster_name']+'-tls'))

logger.info("Lancement des préparations des noeuds (parallelisé)")
with Pool() as p:
    resultats_preparations = p.map(prepare_elastic, [(1,home, dict_config), (2,home, dict_config), (3,home, dict_config)])
logger.info("Préparations terminées pour tous les noeuds")

logger.info("Lancement des noeuds ElasticSearch (parallelisé)")
with Pool() as p:
    resultats_lancement = p.map(run_elastic, [(1,dict_config), (2,dict_config), (3,dict_config)])
logger.info("Noeuds ElasticSearch lancés")

try:
    logger.info("Téléchargement de Kibana version {version}".format(version=dict_config['versionKibana']))
    download_file(
        url="{prefix}/kibana/kibana-{version}-linux-x86_64.tar.gz".format(
            prefix=dict_config['downloadPrefix'],
            version=dict_config['versionKibana']
        ),
        save_dir=elastic_download_folder,
        filename='kibana-{version}-linux-x86_64.tar.gz'.format(version=dict_config['versionKibana'])
    )
    logger.info("Téléchargement terminé: kibana-{version}-linux-x86_64.tar.gz".format(version=dict_config['versionKibana']))
except Exception as e:
    logger.error("Impossible de télécharger l'archive Kibana")
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Impossible de télécharger l'archive Kibana ") from e

try:
    logger.info("Téléchargement du fichier de checksum pour Kibana")
    download_file(
        url="{prefix}/kibana/kibana-{version}-linux-x86_64.tar.gz.sha512".format(
            prefix=dict_config['downloadPrefix'],
            version=dict_config['versionKibana']
        ),
        save_dir=elastic_download_folder,
        filename='kibana-{version}-linux-x86_64.tar.gz.sha512'.format(version=dict_config['versionKibana'])
    )
    logger.info("Téléchargement terminé: checksum kibana-{version}-linux-x86_64.tar.gz.sha512".format(version=dict_config['versionKibana']))
except Exception as e:
    logger.error("Impossible de télécharger l'archive Kibana")
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Impossible de télécharger l'archive Kibana") from e

logger.info("Vérification des checksums pour Kibana")
if not verify_checksums(elastic_download_folder, 'kibana-{version}-linux-x86_64.tar.gz.sha512'.format(version=dict_config['versionKibana'])):
    logger.error("Checksum invalide pour Kibana")
    raise RuntimeError("Le fichier téléchargé n'a pas été téléchargé correctement")
logger.info("Checksum OK pour Kibana")

try:
    logger.info("Décompression de l'archive Kibana")
    with tarfile.open(elastic_download_folder / 'kibana-{version}-linux-x86_64.tar.gz'.format(version=dict_config['versionKibana']), "r:gz") as tar:
        tar.extractall(
            path=elastic_download_folder / 'kibana-{version}-linux-x86_64'.format(version=dict_config['versionKibana']),
            filter='fully_trusted'
        )
    logger.info("Décompression Kibana terminée")
except Exception as e:
    logger.error("Impossible de décompresser l'archive Kibana")
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Impossible de décompresser l'archive Kibana") from e

logger.info("Configuration du mot de passe pour l'utilisateur kibana_system")
password_kibana = generate_password(25)
resp = requests.post(
    url = "https://{cluster_name}-node-1.{domain}/_security/user/kibana_system/_password".format(
    domain = dict_config['ingressDomain'],
    cluster_name = dict_config['cluster_name']
    ),
    auth=("elastic", dict_config['elastic_password']),
    json={"password": password_kibana}
)
logger.info("Mot de passe kibana_system configuré via l'API (status: {code})".format(code=resp.status_code))

kibana_keystore_path = Path(elastic_download_folder) / 'kibana-{version}-linux-x86_64'.format(version=dict_config['versionKibana']) / 'kibana-{version}'.format(version=dict_config['versionKibana']) / 'bin' / 'kibana-keystore'
kibana_server_path =  Path(elastic_download_folder) / 'kibana-{version}-linux-x86_64'.format(version=dict_config['versionKibana']) / 'kibana-{version}'.format(version=dict_config['versionKibana']) / 'bin' / 'kibana'
logger.info("Création du keystore Kibana")
result = subprocess.run(
    [
       str(kibana_keystore_path.resolve()), 
        "create"
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0 or result.stderr:
    logger.error("Kibana keystore creation failed: rc:{rc} stderr:{stderr}".format(rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Echec creation kibana keystore")
logger.info("Kibana keystore created stdout:{out}".format(out=result.stdout.strip()[:1000]))


logger.info("Ajout du mot de passe kibana_system dans le keystore Kibana")
result = subprocess.run(
    [
        '/bin/sh',
        '-c',
        'echo -n \'{password}\' | {keystore} add elasticsearch.password --stdin'.format(
            password = password_kibana,
            keystore = str(kibana_keystore_path.resolve())
        )
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0 or result.stderr:
    logger.error("Adding password to kibana keystore failed: rc:{rc} stderr:{stderr}".format(rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Echec ajout mot de passe kibana_system keystore")
logger.info("Ajout du mot de passe dans le keystore Kibana OK stdout:{out}".format(out=result.stdout.strip()[:1000]))


logger.info("Ajout de l'adresse d'ElasticSearch le keystore Kibana")
result = subprocess.run(
    [
        '/bin/sh',
        '-c',
        'echo -n \'{hosts}\' | {keystore} add elasticsearch.hosts --stdin'.format(
            hosts = 'https://{cluster_name}-node-1.{domain}'.format(cluster_name=dict_config['cluster_name'], domain=dict_config['ingressDomain']),
            keystore = str(kibana_keystore_path.resolve())
        )
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0 or result.stderr:
    logger.error("Adding password to kibana keystore failed: rc:{rc} stderr:{stderr}".format(rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Echec ajout mot de passe kibana_system keystore")
logger.info("Ajout du mot de passe dans le keystore Kibana OK stdout:{out}".format(out=result.stdout.strip()[:1000]))


from_file = Path(dict_config['config_directory']) / 'kibana.yml'
to_file = Path(elastic_download_folder) / 'kibana-{version}-linux-x86_64'.format(version=dict_config['versionKibana']) / 'kibana-{version}'.format(version=dict_config['versionKibana']) / 'config' / 'kibana.yml'

try:
    logger.info("Copie du fichier de configuration Kibana vers {to}".format(to=str(to_file.resolve())))
    shutil.copyfile(from_file, to_file)
    logger.info("Copie Kibana terminée")
except Exception as e:
    logger.error("Impossible de copier le fichier {f1} à l'emplacement {f2}".format(f1=str(from_file.resolve()), f2=str(to_file.resolve())))
    logger.error(e, stack_info=True, exc_info=True)
    raise RuntimeError("Erreur lors de la copie d'un fichier") from e

logger.info("Démarrage de Kibana en arrière-plan")
result = subprocess.run(
    [
        '/bin/sh',
        '-c',
        'nohup {kibana_bin} > /home/onyxia/work/kibana.log 2>&1 &'.format(
            kibana_bin=str(kibana_server_path.resolve())
        )
    ],    
    capture_output=True, 
    text=True 
)
if result.returncode != 0:
    logger.error("Lancement de Kibana echoué rc:{rc} stderr:{stderr}".format(rc=result.returncode, stderr=result.stderr.strip()))
    raise RuntimeError("Echec démarrage Kibana")
logger.info("Commande de démarrage Kibana exécutée (rc:{rc})".format(rc=result.returncode))
